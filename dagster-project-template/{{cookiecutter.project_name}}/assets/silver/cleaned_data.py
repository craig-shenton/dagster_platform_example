"""
Silver layer assets for data cleaning and validation.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue, AssetIn

from platform_core.sdk.decorators.asset_decorators import silver_asset
from platform_core.sdk.decorators.compute_decorators import fargate_compute
from platform_core.asset_checks.data_quality_checks import (
    check_no_nulls,
    check_row_count,
    check_unique_values,
    check_column_values,
)


@silver_asset(
    name="cleaned_customer_data",
    description="Cleaned and validated customer data",
    data_quality_checks=["no_nulls", "row_count"],
    metadata={
        "quality_tier": "silver",
        "validation_rules": ["email_format", "phone_format", "required_fields"],
    },
)
@fargate_compute(cpu_units=512, memory_mb=1024)
def cleaned_customer_data(
    context: AssetExecutionContext,
    raw_customer_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean and validate customer data from the bronze layer.
    
    This asset performs data quality checks and standardization
    on raw customer data to prepare it for analysis.
    """
    
    context.log.info(f"Starting customer data cleaning with {len(raw_customer_data)} records")
    
    df = raw_customer_data.copy()
    
    # Track cleaning operations
    cleaning_stats = {
        "initial_records": len(df),
        "operations_performed": [],
    }
    
    # Remove duplicates based on customer_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['customer_id'])
    duplicates_removed = initial_count - len(df)
    if duplicates_removed > 0:
        cleaning_stats["operations_performed"].append(f"Removed {duplicates_removed} duplicates")
    
    # Clean email addresses
    df['email'] = df['email'].str.lower().str.strip()
    
    # Validate email format
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    invalid_emails = ~df['email'].str.match(email_pattern, na=False)
    df.loc[invalid_emails, 'email'] = None
    invalid_email_count = invalid_emails.sum()
    if invalid_email_count > 0:
        cleaning_stats["operations_performed"].append(f"Invalidated {invalid_email_count} malformed emails")
    
    # Clean phone numbers
    df['phone'] = df['phone'].str.replace(r'[^\d+]', '', regex=True)
    
    # Standardize country names
    country_mapping = {
        'UK': 'United Kingdom',
        'USA': 'United States',
        'US': 'United States',
    }
    df['country'] = df['country'].replace(country_mapping)
    
    # Convert timestamps
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    
    # Remove records with missing required fields
    required_fields = ['customer_id', 'first_name', 'last_name']
    initial_count = len(df)
    df = df.dropna(subset=required_fields)
    missing_required = initial_count - len(df)
    if missing_required > 0:
        cleaning_stats["operations_performed"].append(f"Removed {missing_required} records with missing required fields")
    
    cleaning_stats["final_records"] = len(df)
    
    # Perform data quality checks
    quality_results = []
    
    # Check for nulls in required fields
    null_check = check_no_nulls(df, required_fields)
    quality_results.append(null_check)
    
    # Check row count
    row_count_check = check_row_count(df, min_rows=1)
    quality_results.append(row_count_check)
    
    # Check unique customer IDs
    unique_check = check_unique_values(df, ['customer_id'])
    quality_results.append(unique_check)
    
    # Log quality check results
    for result in quality_results:
        if result.passed:
            context.log.info(f"Quality check passed: {result.description}")
        else:
            context.log.warning(f"Quality check failed: {result.description}")
    
    # Add metadata about the cleaning
    context.add_output_metadata({
        "cleaning_stats": MetadataValue.json(cleaning_stats),
        "quality_checks": MetadataValue.json([
            {"description": r.description, "passed": r.passed} for r in quality_results
        ]),
        "unique_countries": MetadataValue.json(df['country'].unique().tolist()),
        "date_range": MetadataValue.json({
            "min_created": df['created_at'].min().isoformat(),
            "max_created": df['created_at'].max().isoformat(),
        }),
    })
    
    context.log.info(f"Customer data cleaning completed: {len(df)} records")
    
    return df


@silver_asset(
    name="cleaned_order_data",
    description="Cleaned and validated order data",
    data_quality_checks=["no_nulls", "row_count"],
    metadata={
        "quality_tier": "silver",
        "validation_rules": ["positive_amounts", "valid_statuses", "date_consistency"],
    },
)
@fargate_compute(cpu_units=512, memory_mb=1024)
def cleaned_order_data(
    context: AssetExecutionContext,
    raw_order_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean and validate order data from the bronze layer.
    
    This asset performs data quality checks and standardization
    on raw order data to ensure consistency and accuracy.
    """
    
    context.log.info(f"Starting order data cleaning with {len(raw_order_data)} records")
    
    df = raw_order_data.copy()
    
    # Track cleaning operations
    cleaning_stats = {
        "initial_records": len(df),
        "operations_performed": [],
    }
    
    # Convert timestamps
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Validate order amounts
    initial_count = len(df)
    df = df[df['unit_price'] > 0]
    df = df[df['quantity'] > 0]
    df = df[df['line_total'] > 0]
    negative_amounts_removed = initial_count - len(df)
    if negative_amounts_removed > 0:
        cleaning_stats["operations_performed"].append(f"Removed {negative_amounts_removed} records with negative amounts")
    
    # Validate order statuses
    valid_statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
    invalid_status_mask = ~df['order_status'].isin(valid_statuses)
    df.loc[invalid_status_mask, 'order_status'] = 'unknown'
    invalid_status_count = invalid_status_mask.sum()
    if invalid_status_count > 0:
        cleaning_stats["operations_performed"].append(f"Standardized {invalid_status_count} invalid order statuses")
    
    # Recalculate line totals to ensure consistency
    df['calculated_line_total'] = df['quantity'] * df['unit_price']
    inconsistent_totals = abs(df['line_total'] - df['calculated_line_total']) > 0.01
    df.loc[inconsistent_totals, 'line_total'] = df.loc[inconsistent_totals, 'calculated_line_total']
    inconsistent_count = inconsistent_totals.sum()
    if inconsistent_count > 0:
        cleaning_stats["operations_performed"].append(f"Corrected {inconsistent_count} inconsistent line totals")
    
    # Drop the temporary calculation column
    df = df.drop('calculated_line_total', axis=1)
    
    # Remove records with missing required fields
    required_fields = ['order_id', 'customer_id', 'product_id', 'order_date']
    initial_count = len(df)
    df = df.dropna(subset=required_fields)
    missing_required = initial_count - len(df)
    if missing_required > 0:
        cleaning_stats["operations_performed"].append(f"Removed {missing_required} records with missing required fields")
    
    cleaning_stats["final_records"] = len(df)
    
    # Perform data quality checks
    quality_results = []
    
    # Check for nulls in required fields
    null_check = check_no_nulls(df, required_fields)
    quality_results.append(null_check)
    
    # Check row count
    row_count_check = check_row_count(df, min_rows=1)
    quality_results.append(row_count_check)
    
    # Check valid order statuses
    status_check = check_column_values(df, 'order_status', valid_statuses + ['unknown'])
    quality_results.append(status_check)
    
    # Log quality check results
    for result in quality_results:
        if result.passed:
            context.log.info(f"Quality check passed: {result.description}")
        else:
            context.log.warning(f"Quality check failed: {result.description}")
    
    # Add metadata about the cleaning
    context.add_output_metadata({
        "cleaning_stats": MetadataValue.json(cleaning_stats),
        "quality_checks": MetadataValue.json([
            {"description": r.description, "passed": r.passed} for r in quality_results
        ]),
        "order_summary": MetadataValue.json({
            "total_orders": df['order_id'].nunique(),
            "total_customers": df['customer_id'].nunique(),
            "total_products": df['product_id'].nunique(),
            "total_revenue": df['line_total'].sum(),
        }),
        "status_distribution": MetadataValue.json(df['order_status'].value_counts().to_dict()),
    })
    
    context.log.info(f"Order data cleaning completed: {len(df)} records")
    
    return df


@silver_asset(
    name="cleaned_product_catalog",
    description="Cleaned and validated product catalog",
    data_quality_checks=["no_nulls", "row_count"],
    metadata={
        "quality_tier": "silver",
        "validation_rules": ["positive_prices", "valid_categories", "required_fields"],
    },
)
@fargate_compute(cpu_units=256, memory_mb=512)
def cleaned_product_catalog(
    context: AssetExecutionContext,
    raw_product_catalog: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean and validate product catalog data from the bronze layer.
    
    This asset standardizes product information and ensures
    data quality for downstream analysis.
    """
    
    context.log.info(f"Starting product catalog cleaning with {len(raw_product_catalog)} records")
    
    df = raw_product_catalog.copy()
    
    # Track cleaning operations
    cleaning_stats = {
        "initial_records": len(df),
        "operations_performed": [],
    }
    
    # Remove duplicates based on product_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['product_id'])
    duplicates_removed = initial_count - len(df)
    if duplicates_removed > 0:
        cleaning_stats["operations_performed"].append(f"Removed {duplicates_removed} duplicates")
    
    # Clean product names
    df['product_name'] = df['product_name'].str.strip()
    
    # Validate and clean prices
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    initial_count = len(df)
    df = df[df['price'] > 0]
    invalid_prices_removed = initial_count - len(df)
    if invalid_prices_removed > 0:
        cleaning_stats["operations_performed"].append(f"Removed {invalid_prices_removed} records with invalid prices")
    
    # Standardize categories
    df['category'] = df['category'].str.lower().str.strip()
    
    # Remove records with missing required fields
    required_fields = ['product_id', 'product_name', 'category', 'price']
    initial_count = len(df)
    df = df.dropna(subset=required_fields)
    missing_required = initial_count - len(df)
    if missing_required > 0:
        cleaning_stats["operations_performed"].append(f"Removed {missing_required} records with missing required fields")
    
    cleaning_stats["final_records"] = len(df)
    
    # Perform data quality checks
    quality_results = []
    
    # Check for nulls in required fields
    null_check = check_no_nulls(df, required_fields)
    quality_results.append(null_check)
    
    # Check row count
    row_count_check = check_row_count(df, min_rows=1)
    quality_results.append(row_count_check)
    
    # Check unique product IDs
    unique_check = check_unique_values(df, ['product_id'])
    quality_results.append(unique_check)
    
    # Log quality check results
    for result in quality_results:
        if result.passed:
            context.log.info(f"Quality check passed: {result.description}")
        else:
            context.log.warning(f"Quality check failed: {result.description}")
    
    # Add metadata about the cleaning
    context.add_output_metadata({
        "cleaning_stats": MetadataValue.json(cleaning_stats),
        "quality_checks": MetadataValue.json([
            {"description": r.description, "passed": r.passed} for r in quality_results
        ]),
        "product_summary": MetadataValue.json({
            "total_products": len(df),
            "unique_categories": df['category'].nunique(),
            "avg_price": df['price'].mean(),
            "price_range": {
                "min": df['price'].min(),
                "max": df['price'].max(),
            },
        }),
        "category_distribution": MetadataValue.json(df['category'].value_counts().to_dict()),
    })
    
    context.log.info(f"Product catalog cleaning completed: {len(df)} records")
    
    return df