# Compute Orchestration

Dagster's intelligent compute orchestration optimizes resource usage and execution scheduling.

## Execution Architecture

### Dagster Daemon
The daemon process handles:
- **Scheduling**: Cron-based and event-driven schedules
- **Sensors**: File system and external system monitoring
- **Backfills**: Historical data processing
- **Asset Materialization**: Dependency-aware execution

### Compute Resources

#### Local Execution
```python
from dagster import local_executor

@job(executor_def=local_executor)
def my_job():
    pass
```

#### Docker Execution
```python
from dagster_docker import docker_executor

@job(executor_def=docker_executor)
def containerized_job():
    pass
```

#### Kubernetes Execution
```python
from dagster_k8s import k8s_job_executor

@job(executor_def=k8s_job_executor)
def k8s_job():
    pass
```

## Asset Execution

### Dependency Resolution
Dagster automatically resolves asset dependencies:

```python
@asset
def upstream_asset():
    return "data"

@asset(deps=[upstream_asset])
def downstream_asset():
    # Executes after upstream_asset
    pass
```

### Partitioned Execution
Handle large datasets with partitions:

```python
@asset(
    partitions_def=DailyPartitionsDefinition(
        start_date="2023-01-01"
    )
)
def daily_sales_data(context: AssetExecutionContext):
    partition_key = context.asset_partition_key_for_output()
    # Process data for specific date
    pass
```

## Scheduling

### Cron Schedules
```python
from dagster import ScheduleDefinition, job

@job
def daily_job():
    pass

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 1 * * *",  # 1 AM daily
    execution_timezone="UTC"
)
```

### Asset Schedules
```python
from dagster import AssetSelection, define_asset_job

asset_job = define_asset_job(
    "bronze_layer_job",
    selection=AssetSelection.groups("bronze")
)

bronze_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours
)
```

## Sensors

### File System Sensors
```python
from dagster import sensor, RunRequest

@sensor(job=data_processing_job)
def file_sensor(context):
    # Check for new files
    if new_files_available():
        yield RunRequest()
```

### External System Sensors
```python
@sensor(job=api_ingestion_job)
def api_sensor(context):
    # Poll external API
    if api_has_new_data():
        yield RunRequest(
            run_config={
                "resources": {
                    "api_client": {"config": {"endpoint": "..."}}
                }
            }
        )
```

## Resource Management

### Compute Resources
```python
from dagster import ConfigurableResource

class ComputeResource(ConfigurableResource):
    cpu_limit: str = "2000m"
    memory_limit: str = "4Gi"
    
    def get_k8s_config(self):
        return {
            "container_config": {
                "resources": {
                    "requests": {
                        "cpu": self.cpu_limit,
                        "memory": self.memory_limit
                    }
                }
            }
        }
```

### Auto-scaling
```python
@asset(
    compute_kind="spark",
    resource_defs={
        "spark": SparkResource(
            executor_instances=lambda context: 
                scale_based_on_data_size(context.asset_partition_key)
        )
    }
)
def large_dataset_processing(context, spark):
    # Dynamic scaling based on data size
    pass
```

## Monitoring & Observability

### Execution Metrics
- **Duration**: Track execution time
- **Resource Usage**: Monitor CPU/memory
- **Success Rate**: Track failure rates
- **Throughput**: Monitor data volume

### Alerting
```python
from dagster import success_hook, failure_hook

@success_hook
def on_success(context):
    send_success_notification(context)

@failure_hook
def on_failure(context):
    send_failure_alert(context)
```

## Performance Optimization

### Parallel Execution
```python
# Enable parallel execution
from dagster import multiprocess_executor

@job(executor_def=multiprocess_executor)
def parallel_job():
    pass
```

### Resource Optimization
1. **Right-sizing**: Match resources to workload
2. **Caching**: Use asset caching for expensive operations
3. **Incremental Processing**: Process only changed data
4. **Lazy Loading**: Load data only when needed

## Related Documentation

- [Platform Overview](platform-overview.md)
- [Resource Management](resource-management.md)
- [Performance Optimization](../best-practices/performance-optimization.md)