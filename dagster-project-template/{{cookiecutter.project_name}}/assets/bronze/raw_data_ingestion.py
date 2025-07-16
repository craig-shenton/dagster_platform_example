"""
Bronze layer assets for raw data ingestion.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue

from platform_core.resources.database_resources import PostgresResource
from platform_core.resources.aws_resources import S3Resource
from platform_core.sdk.decorators.asset_decorators import bronze_asset
from platform_core.sdk.decorators.compute_decorators import lambda_compute


@bronze_asset(
    name="raw_customer_data",
    description="Raw customer data from source system",
    metadata={
        "source": "customer_database",
        "update_frequency": "daily",
    },
)
@lambda_compute(timeout_seconds=300, memory_mb=256)
def raw_customer_data(context: AssetExecutionContext, database: PostgresResource) -> pd.DataFrame:
    """
    Ingest raw customer data from the source database.
    
    This asset extracts customer information from the operational database
    and stores it in the bronze layer for further processing.
    """
    
    query = """
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        country,
        created_at,
        updated_at
    FROM customers
    WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    context.log.info("Extracting raw customer data")
    df = database.execute_query(query)
    
    # Add metadata about the ingestion
    context.add_output_metadata({
        "num_records": MetadataValue.int(len(df)),
        "columns": MetadataValue.json(df.columns.tolist()),
        "data_types": MetadataValue.json(df.dtypes.astype(str).to_dict()),
        "memory_usage_mb": MetadataValue.float(df.memory_usage(deep=True).sum() / 1024 / 1024),
    })
    
    return df


@bronze_asset(
    name="raw_order_data",
    description="Raw order data from source system",
    metadata={
        "source": "order_database",
        "update_frequency": "hourly",
    },
)
@lambda_compute(timeout_seconds=600, memory_mb=512)
def raw_order_data(context: AssetExecutionContext, database: PostgresResource) -> pd.DataFrame:
    """
    Ingest raw order data from the source database.
    
    This asset extracts order information including order details
    and line items from the operational database.
    """
    
    query = """
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.total_amount,
        o.currency,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '1 day'
    """
    
    context.log.info("Extracting raw order data")
    df = database.execute_query(query)
    
    # Add metadata about the ingestion
    context.add_output_metadata({
        "num_records": MetadataValue.int(len(df)),
        "unique_orders": MetadataValue.int(df['order_id'].nunique()),
        "unique_customers": MetadataValue.int(df['customer_id'].nunique()),
        "total_revenue": MetadataValue.float(df['line_total'].sum()),
        "date_range": MetadataValue.json({
            "min_date": df['order_date'].min().isoformat(),
            "max_date": df['order_date'].max().isoformat(),
        }),
    })
    
    return df


@bronze_asset(
    name="raw_product_catalog",
    description="Raw product catalog data from S3",
    metadata={
        "source": "s3_data_lake",
        "update_frequency": "daily",
    },
)
@lambda_compute(timeout_seconds=300, memory_mb=256)
def raw_product_catalog(context: AssetExecutionContext, s3: S3Resource) -> pd.DataFrame:
    """
    Ingest raw product catalog data from S3.
    
    This asset reads product information from files stored in S3
    and prepares it for processing in the bronze layer.
    """
    
    # List product files in S3
    product_files = s3.list_objects(prefix="product_catalog/")
    
    if not product_files:
        context.log.warning("No product files found in S3")
        return pd.DataFrame()
    
    # Read and combine all product files
    all_products = []
    
    for file_key in product_files:
        if file_key.endswith('.csv'):
            context.log.info(f"Processing file: {file_key}")
            
            # Download file locally
            local_path = f"/tmp/{file_key.split('/')[-1]}"
            s3.download_file(file_key, local_path)
            
            # Read CSV file
            df = pd.read_csv(local_path)
            df['source_file'] = file_key
            all_products.append(df)
    
    if not all_products:
        context.log.warning("No CSV files found in product catalog")
        return pd.DataFrame()
    
    # Combine all product data
    combined_df = pd.concat(all_products, ignore_index=True)
    
    # Add metadata about the ingestion
    context.add_output_metadata({
        "num_records": MetadataValue.int(len(combined_df)),
        "unique_products": MetadataValue.int(combined_df['product_id'].nunique()),
        "source_files": MetadataValue.json(product_files),
        "categories": MetadataValue.json(combined_df['category'].unique().tolist()),
    })
    
    return combined_df


@bronze_asset(
    name="raw_web_events",
    description="Raw web event data from streaming source",
    metadata={
        "source": "kinesis_stream",
        "update_frequency": "real_time",
    },
)
@lambda_compute(timeout_seconds=900, memory_mb=512)
def raw_web_events(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Ingest raw web event data from streaming source.
    
    This asset processes web events from a streaming source
    and batches them for bronze layer storage.
    """
    
    # In a real implementation, this would connect to Kinesis or similar
    # For this example, we'll simulate some web event data
    import random
    from datetime import datetime, timedelta
    
    context.log.info("Simulating web event data ingestion")
    
    # Generate sample web events
    events = []
    for i in range(1000):
        events.append({
            'event_id': f"event_{i}",
            'user_id': f"user_{random.randint(1, 100)}",
            'event_type': random.choice(['page_view', 'click', 'purchase', 'search']),
            'page_url': f"/page_{random.randint(1, 20)}",
            'timestamp': datetime.now() - timedelta(minutes=random.randint(0, 60)),
            'session_id': f"session_{random.randint(1, 200)}",
            'user_agent': 'Mozilla/5.0 (compatible; example)',
        })
    
    df = pd.DataFrame(events)
    
    # Add metadata about the ingestion
    context.add_output_metadata({
        "num_events": MetadataValue.int(len(df)),
        "unique_users": MetadataValue.int(df['user_id'].nunique()),
        "event_types": MetadataValue.json(df['event_type'].value_counts().to_dict()),
        "time_range": MetadataValue.json({
            "min_time": df['timestamp'].min().isoformat(),
            "max_time": df['timestamp'].max().isoformat(),
        }),
    })
    
    return df