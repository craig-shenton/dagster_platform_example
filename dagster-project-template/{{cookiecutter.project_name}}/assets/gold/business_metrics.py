"""
Gold layer assets for business logic and analytics.
"""

import pandas as pd
from dagster import asset, AssetExecutionContext, MetadataValue, AssetIn

from platform_core.sdk.decorators.asset_decorators import gold_asset
from platform_core.sdk.decorators.compute_decorators import eks_compute


@gold_asset(
    name="customer_lifetime_value",
    description="Customer lifetime value calculations",
    business_owner="analytics_team",
    metadata={
        "business_purpose": "Customer segmentation and retention analysis",
        "update_frequency": "daily",
        "kpi_tier": "tier_1",
    },
)
@eks_compute(node_type="m5.large", min_nodes=2, max_nodes=5)
def customer_lifetime_value(
    context: AssetExecutionContext,
    cleaned_customer_data: pd.DataFrame,
    cleaned_order_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate customer lifetime value (CLV) for all customers.
    
    This asset combines customer and order data to compute
    key metrics for customer value analysis.
    """
    
    context.log.info("Starting customer lifetime value calculation")
    
    # Aggregate order data by customer
    customer_orders = cleaned_order_data.groupby('customer_id').agg({
        'order_id': 'nunique',
        'line_total': ['sum', 'mean'],
        'order_date': ['min', 'max'],
        'quantity': 'sum',
    }).reset_index()
    
    # Flatten column names
    customer_orders.columns = [
        'customer_id', 'total_orders', 'total_spent', 'avg_order_value',
        'first_order_date', 'last_order_date', 'total_items_purchased'
    ]
    
    # Calculate customer tenure in days
    customer_orders['tenure_days'] = (
        customer_orders['last_order_date'] - customer_orders['first_order_date']
    ).dt.days
    
    # Handle single-order customers (tenure = 0)
    customer_orders['tenure_days'] = customer_orders['tenure_days'].fillna(0)
    
    # Calculate purchase frequency (orders per day)
    customer_orders['purchase_frequency'] = customer_orders['total_orders'] / (
        customer_orders['tenure_days'] + 1  # Add 1 to avoid division by zero
    )
    
    # Join with customer data
    clv_df = cleaned_customer_data.merge(
        customer_orders,
        on='customer_id',
        how='left'
    )
    
    # Fill NaN values for customers with no orders
    order_columns = ['total_orders', 'total_spent', 'avg_order_value', 'total_items_purchased']
    clv_df[order_columns] = clv_df[order_columns].fillna(0)
    
    # Calculate CLV components
    clv_df['avg_order_value'] = clv_df['avg_order_value'].fillna(0)
    clv_df['purchase_frequency'] = clv_df['purchase_frequency'].fillna(0)
    
    # Simple CLV calculation: AOV * Purchase Frequency * Estimated Lifetime (365 days)
    clv_df['estimated_clv'] = (
        clv_df['avg_order_value'] * 
        clv_df['purchase_frequency'] * 
        365
    )
    
    # Customer segmentation based on CLV
    clv_df['clv_segment'] = pd.cut(
        clv_df['estimated_clv'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['Low', 'Medium', 'High', 'VIP']
    )
    
    # Calculate recency (days since last order)
    today = pd.Timestamp.now()
    clv_df['recency_days'] = (today - clv_df['last_order_date']).dt.days
    clv_df['recency_days'] = clv_df['recency_days'].fillna(999)  # For customers with no orders
    
    # Select final columns
    final_columns = [
        'customer_id', 'first_name', 'last_name', 'email', 'country',
        'total_orders', 'total_spent', 'avg_order_value', 'total_items_purchased',
        'first_order_date', 'last_order_date', 'tenure_days', 'purchase_frequency',
        'estimated_clv', 'clv_segment', 'recency_days'
    ]
    
    clv_df = clv_df[final_columns]
    
    # Add metadata about the calculation
    context.add_output_metadata({
        "total_customers": MetadataValue.int(len(clv_df)),
        "customers_with_orders": MetadataValue.int(len(clv_df[clv_df['total_orders'] > 0])),
        "clv_statistics": MetadataValue.json({
            "avg_clv": clv_df['estimated_clv'].mean(),
            "median_clv": clv_df['estimated_clv'].median(),
            "max_clv": clv_df['estimated_clv'].max(),
            "total_clv": clv_df['estimated_clv'].sum(),
        }),
        "segment_distribution": MetadataValue.json(
            clv_df['clv_segment'].value_counts().to_dict()
        ),
        "top_customers": MetadataValue.json(
            clv_df.nlargest(10, 'estimated_clv')[['customer_id', 'estimated_clv']].to_dict('records')
        ),
    })
    
    context.log.info(f"Customer lifetime value calculation completed for {len(clv_df)} customers")
    
    return clv_df


@gold_asset(
    name="product_performance",
    description="Product performance metrics and analysis",
    business_owner="product_team",
    metadata={
        "business_purpose": "Product optimization and inventory management",
        "update_frequency": "daily",
        "kpi_tier": "tier_1",
    },
)
@eks_compute(node_type="m5.large", min_nodes=1, max_nodes=3)
def product_performance(
    context: AssetExecutionContext,
    cleaned_order_data: pd.DataFrame,
    cleaned_product_catalog: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate product performance metrics.
    
    This asset analyzes product sales data to provide insights
    into product performance and profitability.
    """
    
    context.log.info("Starting product performance analysis")
    
    # Aggregate order data by product
    product_sales = cleaned_order_data.groupby('product_id').agg({
        'order_id': 'nunique',
        'customer_id': 'nunique',
        'quantity': 'sum',
        'line_total': ['sum', 'mean'],
        'order_date': ['min', 'max', 'count'],
    }).reset_index()
    
    # Flatten column names
    product_sales.columns = [
        'product_id', 'unique_orders', 'unique_customers', 'total_quantity_sold',
        'total_revenue', 'avg_revenue_per_order', 'first_sale_date', 'last_sale_date', 'total_order_lines'
    ]
    
    # Calculate additional metrics
    product_sales['avg_quantity_per_order'] = (
        product_sales['total_quantity_sold'] / product_sales['total_order_lines']
    )
    
    # Calculate sales velocity (orders per day)
    product_sales['sales_period_days'] = (
        product_sales['last_sale_date'] - product_sales['first_sale_date']
    ).dt.days + 1  # Add 1 to avoid division by zero
    
    product_sales['sales_velocity'] = (
        product_sales['total_order_lines'] / product_sales['sales_period_days']
    )
    
    # Join with product catalog
    performance_df = cleaned_product_catalog.merge(
        product_sales,
        on='product_id',
        how='left'
    )
    
    # Fill NaN values for products with no sales
    sales_columns = [
        'unique_orders', 'unique_customers', 'total_quantity_sold',
        'total_revenue', 'avg_revenue_per_order', 'total_order_lines',
        'avg_quantity_per_order', 'sales_velocity'
    ]
    performance_df[sales_columns] = performance_df[sales_columns].fillna(0)
    
    # Calculate profit margin (assuming 30% margin for simplicity)
    performance_df['estimated_profit'] = performance_df['total_revenue'] * 0.30
    
    # Product performance scoring
    performance_df['revenue_rank'] = performance_df['total_revenue'].rank(ascending=False)
    performance_df['quantity_rank'] = performance_df['total_quantity_sold'].rank(ascending=False)
    performance_df['velocity_rank'] = performance_df['sales_velocity'].rank(ascending=False)
    
    # Combined performance score (lower is better)
    performance_df['performance_score'] = (
        performance_df['revenue_rank'] + 
        performance_df['quantity_rank'] + 
        performance_df['velocity_rank']
    ) / 3
    
    # Product segmentation
    performance_df['performance_segment'] = pd.cut(
        performance_df['performance_score'],
        bins=[0, 50, 150, 300, float('inf')],
        labels=['Star', 'Good', 'Average', 'Poor']
    )
    
    # Calculate inventory turnover (assuming current inventory equals avg monthly sales)
    monthly_sales = performance_df['total_quantity_sold'] / 12  # Assuming 12 months of data
    performance_df['estimated_inventory_turnover'] = monthly_sales
    
    # Select final columns
    final_columns = [
        'product_id', 'product_name', 'category', 'price',
        'unique_orders', 'unique_customers', 'total_quantity_sold',
        'total_revenue', 'avg_revenue_per_order', 'avg_quantity_per_order',
        'sales_velocity', 'estimated_profit', 'performance_score',
        'performance_segment', 'estimated_inventory_turnover'
    ]
    
    performance_df = performance_df[final_columns]
    
    # Add metadata about the analysis
    context.add_output_metadata({
        "total_products": MetadataValue.int(len(performance_df)),
        "products_with_sales": MetadataValue.int(len(performance_df[performance_df['total_revenue'] > 0])),
        "revenue_statistics": MetadataValue.json({
            "total_revenue": performance_df['total_revenue'].sum(),
            "avg_revenue_per_product": performance_df['total_revenue'].mean(),
            "median_revenue": performance_df['total_revenue'].median(),
            "top_revenue_product": performance_df.loc[performance_df['total_revenue'].idxmax(), 'product_name'],
        }),
        "segment_distribution": MetadataValue.json(
            performance_df['performance_segment'].value_counts().to_dict()
        ),
        "category_performance": MetadataValue.json(
            performance_df.groupby('category')['total_revenue'].sum().to_dict()
        ),
        "top_products": MetadataValue.json(
            performance_df.nlargest(10, 'total_revenue')[['product_name', 'total_revenue']].to_dict('records')
        ),
    })
    
    context.log.info(f"Product performance analysis completed for {len(performance_df)} products")
    
    return performance_df


@gold_asset(
    name="daily_sales_summary",
    description="Daily sales summary and KPIs",
    business_owner="sales_team",
    metadata={
        "business_purpose": "Daily sales monitoring and reporting",
        "update_frequency": "daily",
        "kpi_tier": "tier_1",
    },
)
@eks_compute(node_type="m5.large", min_nodes=1, max_nodes=2)
def daily_sales_summary(
    context: AssetExecutionContext,
    cleaned_order_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate daily sales summary with key performance indicators.
    
    This asset provides daily sales metrics for business monitoring
    and dashboard reporting.
    """
    
    context.log.info("Starting daily sales summary calculation")
    
    # Group by date
    daily_summary = cleaned_order_data.groupby(
        cleaned_order_data['order_date'].dt.date
    ).agg({
        'order_id': 'nunique',
        'customer_id': 'nunique',
        'product_id': 'nunique',
        'quantity': 'sum',
        'line_total': ['sum', 'mean'],
    }).reset_index()
    
    # Flatten column names
    daily_summary.columns = [
        'date', 'total_orders', 'unique_customers', 'unique_products',
        'total_items_sold', 'total_revenue', 'avg_order_value'
    ]
    
    # Calculate additional metrics
    daily_summary['items_per_order'] = (
        daily_summary['total_items_sold'] / daily_summary['total_orders']
    )
    
    daily_summary['revenue_per_customer'] = (
        daily_summary['total_revenue'] / daily_summary['unique_customers']
    )
    
    # Calculate rolling averages (7-day window)
    daily_summary = daily_summary.sort_values('date')
    daily_summary['revenue_7day_avg'] = daily_summary['total_revenue'].rolling(window=7, min_periods=1).mean()
    daily_summary['orders_7day_avg'] = daily_summary['total_orders'].rolling(window=7, min_periods=1).mean()
    daily_summary['customers_7day_avg'] = daily_summary['unique_customers'].rolling(window=7, min_periods=1).mean()
    
    # Calculate day-over-day growth
    daily_summary['revenue_growth'] = daily_summary['total_revenue'].pct_change()
    daily_summary['orders_growth'] = daily_summary['total_orders'].pct_change()
    daily_summary['customers_growth'] = daily_summary['unique_customers'].pct_change()
    
    # Add day of week
    daily_summary['day_of_week'] = pd.to_datetime(daily_summary['date']).dt.day_name()
    
    # Calculate cumulative metrics
    daily_summary['cumulative_revenue'] = daily_summary['total_revenue'].cumsum()
    daily_summary['cumulative_orders'] = daily_summary['total_orders'].cumsum()
    daily_summary['cumulative_customers'] = daily_summary['unique_customers'].cumsum()
    
    # Add metadata about the summary
    context.add_output_metadata({
        "date_range": MetadataValue.json({
            "start_date": daily_summary['date'].min().isoformat(),
            "end_date": daily_summary['date'].max().isoformat(),
            "total_days": len(daily_summary),
        }),
        "summary_statistics": MetadataValue.json({
            "total_revenue": daily_summary['total_revenue'].sum(),
            "avg_daily_revenue": daily_summary['total_revenue'].mean(),
            "best_day_revenue": daily_summary['total_revenue'].max(),
            "worst_day_revenue": daily_summary['total_revenue'].min(),
            "total_orders": daily_summary['total_orders'].sum(),
            "avg_daily_orders": daily_summary['total_orders'].mean(),
        }),
        "growth_metrics": MetadataValue.json({
            "avg_revenue_growth": daily_summary['revenue_growth'].mean(),
            "avg_orders_growth": daily_summary['orders_growth'].mean(),
            "avg_customers_growth": daily_summary['customers_growth'].mean(),
        }),
        "day_of_week_performance": MetadataValue.json(
            daily_summary.groupby('day_of_week')['total_revenue'].mean().to_dict()
        ),
    })
    
    context.log.info(f"Daily sales summary completed for {len(daily_summary)} days")
    
    return daily_summary