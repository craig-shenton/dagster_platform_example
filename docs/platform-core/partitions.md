# Partitions

Partitions enable efficient processing of large datasets by dividing them into manageable chunks.

## Partition Types

### Daily Partitions
```python
from dagster import DailyPartitionsDefinition, asset

daily_partitions = DailyPartitionsDefinition(
    start_date="2023-01-01",
    end_date="2024-12-31",
    timezone="UTC"
)

@asset(partitions_def=daily_partitions)
def daily_sales(context):
    """Process daily sales data."""
    partition_key = context.asset_partition_key_for_output()
    # Process data for specific date
    return process_sales_for_date(partition_key)
```

### Monthly Partitions
```python
from dagster import MonthlyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2023-01-01",
    timezone="UTC"
)

@asset(partitions_def=monthly_partitions)
def monthly_revenue(context):
    """Calculate monthly revenue."""
    partition_key = context.asset_partition_key_for_output()
    return calculate_monthly_revenue(partition_key)
```

### Hourly Partitions
```python
from dagster import HourlyPartitionsDefinition

hourly_partitions = HourlyPartitionsDefinition(
    start_date="2023-01-01-00:00",
    timezone="UTC"
)

@asset(partitions_def=hourly_partitions)
def hourly_metrics(context):
    """Process hourly metrics."""
    partition_key = context.asset_partition_key_for_output()
    return process_hourly_data(partition_key)
```

## Static Partitions

### Region-based Partitions
```python
from dagster import StaticPartitionsDefinition

region_partitions = StaticPartitionsDefinition([
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-southeast-1"
])

@asset(partitions_def=region_partitions)
def regional_data(context):
    """Process data by region."""
    region = context.asset_partition_key_for_output()
    return process_region_data(region)
```

### Environment Partitions
```python
env_partitions = StaticPartitionsDefinition([
    "development",
    "staging",
    "production"
])

@asset(partitions_def=env_partitions)
def environment_config(context):
    """Generate environment-specific configuration."""
    env = context.asset_partition_key_for_output()
    return generate_config_for_env(env)
```

## Multi-dimensional Partitions

### Date and Region Partitions
```python
from dagster import MultiPartitionsDefinition

date_region_partitions = MultiPartitionsDefinition({
    "date": daily_partitions,
    "region": region_partitions
})

@asset(partitions_def=date_region_partitions)
def daily_regional_sales(context):
    """Process daily sales by region."""
    partition_key = context.asset_partition_key_for_output()
    date = partition_key.keys_by_dimension["date"]
    region = partition_key.keys_by_dimension["region"]
    
    return process_sales_data(date, region)
```

## Partition Dependencies

### Upstream Partitions
```python
@asset(partitions_def=daily_partitions)
def raw_data(context):
    """Load raw data."""
    date = context.asset_partition_key_for_output()
    return load_raw_data(date)

@asset(
    partitions_def=daily_partitions,
    deps=[raw_data]
)
def processed_data(context):
    """Process raw data."""
    date = context.asset_partition_key_for_output()
    raw = context.get_asset_value(raw_data, partition_key=date)
    return process_data(raw)
```

### Cross-partition Dependencies
```python
@asset(partitions_def=daily_partitions)
def daily_summary(context):
    """Create daily summary with 7-day lookback."""
    current_date = context.asset_partition_key_for_output()
    
    # Get data for last 7 days
    lookback_data = []
    for i in range(7):
        lookback_date = (pd.Timestamp(current_date) - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        data = context.get_asset_value(raw_data, partition_key=lookback_date)
        lookback_data.append(data)
    
    return create_summary(lookback_data)
```

## Partition Configuration

### Dynamic Partition Keys
```python
from dagster import DynamicPartitionsDefinition

customer_partitions = DynamicPartitionsDefinition(
    name="customer_segments"
)

@asset(partitions_def=customer_partitions)
def customer_segment_analysis(context):
    """Analyze customer segments."""
    segment = context.asset_partition_key_for_output()
    return analyze_segment(segment)

# Add partitions dynamically
from dagster import add_dynamic_partitions

add_dynamic_partitions(
    partitions_def=customer_partitions,
    partition_keys=["high_value", "medium_value", "low_value"]
)
```

### Partition Mapping
```python
from dagster import PartitionMapping

class CustomPartitionMapping(PartitionMapping):
    def get_upstream_partitions_for_partition_range(
        self, downstream_partition_subset
    ):
        # Custom logic to map partitions
        return upstream_partitions

@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("upstream_asset", partition_mapping=CustomPartitionMapping())]
)
def mapped_asset(context):
    """Asset with custom partition mapping."""
    pass
```

## Backfills

### Partition Backfill
```python
from dagster import build_asset_job

# Create job for backfilling
backfill_job = build_asset_job(
    "backfill_daily_data",
    [daily_sales, processed_data],
    partitions_def=daily_partitions
)

# Run backfill for date range
# dagster job backfill --partition-range 2023-01-01:2023-01-31
```

### Selective Backfill
```python
@asset(
    partitions_def=daily_partitions,
    auto_materialize_policy=AutoMaterializePolicy.eager()
)
def selective_processing(context):
    """Only process if source data changed."""
    partition_key = context.asset_partition_key_for_output()
    
    # Check if processing is needed
    if not needs_processing(partition_key):
        return None
    
    return process_data(partition_key)
```

## Partition Maintenance

### Partition Cleanup
```python
from dagster import sensor, RunRequest

@sensor(job=cleanup_job)
def partition_cleanup_sensor(context):
    """Clean up old partitions."""
    cutoff_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    
    old_partitions = get_partitions_before_date(cutoff_date)
    
    if old_partitions:
        yield RunRequest(
            run_config={
                "ops": {
                    "cleanup_partitions": {
                        "config": {
                            "partitions_to_delete": old_partitions
                        }
                    }
                }
            }
        )
```

### Partition Monitoring
```python
@asset_check(asset="daily_sales")
def partition_freshness_check(context):
    """Check if recent partitions are available."""
    latest_partition = get_latest_partition("daily_sales")
    expected_partition = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    if latest_partition < expected_partition:
        return AssetCheckResult(
            passed=False,
            description=f"Missing partition: {expected_partition}"
        )
    
    return AssetCheckResult(
        passed=True,
        description="All recent partitions available"
    )
```

## Partition Performance

### Parallel Processing
```python
from dagster import multiprocess_executor

@job(
    executor_def=multiprocess_executor,
    partitions_def=daily_partitions
)
def parallel_processing_job():
    """Process partitions in parallel."""
    processed_data()
```

### Partition Caching
```python
@asset(
    partitions_def=daily_partitions,
    io_manager_key="cached_io_manager"
)
def cached_processing(context):
    """Cache partition results."""
    partition_key = context.asset_partition_key_for_output()
    
    # Check cache first
    if cached_result := get_cached_result(partition_key):
        return cached_result
    
    # Process and cache
    result = expensive_processing(partition_key)
    cache_result(partition_key, result)
    return result
```

## Testing Partitions

### Partition Testing
```python
def test_daily_sales_partition():
    """Test daily sales partition processing."""
    from dagster import materialize
    
    result = materialize(
        [daily_sales],
        partition_key="2023-01-01"
    )
    
    assert result.success
    assert result.asset_materializations_for_node("daily_sales")
```

### Mock Partition Data
```python
from dagster import build_asset_context

def test_partition_logic():
    """Test partition logic with mock data."""
    context = build_asset_context(
        partition_key="2023-01-01"
    )
    
    result = daily_sales(context)
    assert result is not None
```

## Related Documentation

- [Asset Development](../best-practices/asset-development.md)
- [Performance Optimization](../best-practices/performance-optimization.md)
- [Data Layers](../architecture/data-layers.md)