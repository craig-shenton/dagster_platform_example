# Asset Checks

Asset checks provide automated validation and quality assurance for your data assets.

## Overview

The platform includes comprehensive asset checks for:
- Data quality validation
- Schema compliance
- Freshness monitoring
- Completeness verification

## Data Quality Checks

### Null Value Check
```python
from dagster import asset_check
from platform_core.asset_checks.data_quality_checks import check_null_values

@asset_check(asset="customer_data")
def customer_data_null_check(context):
    """Check for null values in critical fields."""
    return check_null_values(
        context=context,
        asset_key="customer_data",
        columns=["customer_id", "email", "created_at"],
        max_null_percentage=0.01  # Allow 1% nulls
    )
```

### Duplicate Check
```python
@asset_check(asset="customer_data")
def customer_data_duplicate_check(context):
    """Check for duplicate records."""
    return check_duplicates(
        context=context,
        asset_key="customer_data",
        key_columns=["customer_id"],
        max_duplicates=0
    )
```

### Range Check
```python
@asset_check(asset="sales_data")
def sales_amount_range_check(context):
    """Validate sales amount ranges."""
    return check_numeric_range(
        context=context,
        asset_key="sales_data",
        column="amount",
        min_value=0,
        max_value=1000000
    )
```

## Schema Checks

### Column Presence Check
```python
from platform_core.asset_checks.schema_checks import check_required_columns

@asset_check(asset="customer_data")
def customer_schema_check(context):
    """Validate required columns exist."""
    return check_required_columns(
        context=context,
        asset_key="customer_data",
        required_columns=[
            "customer_id",
            "email",
            "first_name",
            "last_name",
            "created_at"
        ]
    )
```

### Data Type Check
```python
@asset_check(asset="customer_data")
def customer_data_types_check(context):
    """Validate column data types."""
    return check_data_types(
        context=context,
        asset_key="customer_data",
        expected_types={
            "customer_id": "int64",
            "email": "string",
            "created_at": "datetime64[ns]"
        }
    )
```

## Freshness Checks

### Data Freshness Check
```python
from platform_core.asset_checks.freshness_checks import check_data_freshness

@asset_check(asset="daily_sales")
def daily_sales_freshness_check(context):
    """Check if daily sales data is fresh."""
    return check_data_freshness(
        context=context,
        asset_key="daily_sales",
        max_age_hours=25,  # Allow 1 hour buffer
        timestamp_column="created_at"
    )
```

### Asset Materialization Freshness
```python
@asset_check(asset="customer_metrics")
def customer_metrics_freshness_check(context):
    """Check when asset was last materialized."""
    return check_materialization_freshness(
        context=context,
        asset_key="customer_metrics",
        max_age_hours=6
    )
```

## Completeness Checks

### Row Count Check
```python
@asset_check(asset="daily_transactions")
def transaction_count_check(context):
    """Validate minimum transaction count."""
    return check_row_count(
        context=context,
        asset_key="daily_transactions",
        min_rows=100,
        max_rows=1000000
    )
```

### Data Completeness Check
```python
@asset_check(asset="customer_profile")
def profile_completeness_check(context):
    """Check profile data completeness."""
    return check_completeness(
        context=context,
        asset_key="customer_profile",
        required_fields=["email", "phone", "address"],
        min_completeness=0.8  # 80% complete
    )
```

## Custom Checks

### Business Logic Check
```python
@asset_check(asset="order_data")
def order_validation_check(context):
    """Custom business logic validation."""
    # Load asset data
    asset_data = context.get_asset_value("order_data")
    
    # Custom validation logic
    invalid_orders = asset_data[
        (asset_data['order_total'] < 0) |
        (asset_data['order_date'] > pd.Timestamp.now()) |
        (asset_data['customer_id'].isnull())
    ]
    
    if len(invalid_orders) > 0:
        return AssetCheckResult(
            passed=False,
            description=f"Found {len(invalid_orders)} invalid orders",
            metadata={
                "invalid_count": len(invalid_orders),
                "total_count": len(asset_data)
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description="All orders passed validation"
    )
```

## Check Scheduling

### Scheduled Checks
```python
from dagster import ScheduleDefinition, define_asset_job

# Create job with checks
data_quality_job = define_asset_job(
    "data_quality_checks",
    selection=["customer_data", "sales_data"],
    asset_check_selection="*"
)

# Schedule checks
data_quality_schedule = ScheduleDefinition(
    job=data_quality_job,
    cron_schedule="0 6 * * *",  # 6 AM daily
    execution_timezone="UTC"
)
```

### Sensor-based Checks
```python
from dagster import sensor, RunRequest

@sensor(job=data_quality_job)
def data_quality_sensor(context):
    """Run checks when new data arrives."""
    if new_data_available():
        yield RunRequest(
            partition_key=get_current_partition()
        )
```

## Check Results

### Handling Failures
```python
from dagster import failure_hook

@failure_hook
def on_check_failure(context):
    """Handle check failures."""
    if context.is_asset_check:
        send_alert(
            f"Asset check failed: {context.asset_check_key}",
            context.failure_event.error_info
        )
```

### Metrics Collection
```python
@asset_check(asset="customer_data")
def customer_data_metrics_check(context):
    """Collect and report metrics."""
    result = check_data_quality(context)
    
    # Report metrics
    context.log_event(
        AssetObservation(
            asset_key="customer_data",
            metadata={
                "quality_score": result.quality_score,
                "null_percentage": result.null_percentage,
                "duplicate_count": result.duplicate_count
            }
        )
    )
    
    return result
```

## Configuration

### Check Configuration
```python
from dagster import Config

class DataQualityConfig(Config):
    max_null_percentage: float = 0.01
    max_duplicates: int = 0
    required_columns: List[str]
    
@asset_check(asset="customer_data")
def configurable_quality_check(context, config: DataQualityConfig):
    """Quality check with configuration."""
    return run_quality_checks(
        context=context,
        config=config
    )
```

## Monitoring

### Check Dashboard
```python
from dagster import MetadataValue

@asset_check(asset="daily_sales")
def sales_monitoring_check(context):
    """Monitor sales data with dashboard metrics."""
    result = check_sales_data(context)
    
    return AssetCheckResult(
        passed=result.passed,
        metadata={
            "sales_count": MetadataValue.int(result.sales_count),
            "revenue_total": MetadataValue.float(result.revenue_total),
            "avg_order_value": MetadataValue.float(result.avg_order_value)
        }
    )
```

### Alerting
```python
@asset_check(asset="critical_data")
def critical_data_check(context):
    """Critical data check with alerting."""
    result = check_critical_data(context)
    
    if not result.passed:
        send_critical_alert(
            subject="Critical Data Check Failed",
            body=f"Check failed: {result.description}"
        )
    
    return result
```

## Related Documentation

- [Data Quality](../governance/data-quality.md)
- [Monitoring](../observability/monitoring.md)
- [Testing](../best-practices/testing.md)