# API Documentation

This page contains API documentation for the Dagster Platform components. The full API documentation will be auto-generated from code docstrings when the platform is installed.

## Platform Core

### Resources

The platform provides several resource types for connecting to external systems:

#### AWS Resources
- **S3Resource**: Interact with AWS S3 buckets for file storage
- **SecretsManagerResource**: Manage secrets from AWS Secrets Manager
- **LambdaResource**: Invoke AWS Lambda functions

#### Database Resources
- **PostgresResource**: Connect to PostgreSQL databases
- **RedshiftResource**: Connect to Amazon Redshift data warehouses
- **SnowflakeResource**: Connect to Snowflake data platforms

#### API Resources
- **HTTPResource**: Make HTTP requests to REST APIs
- **SlackResource**: Send notifications to Slack channels
- **EmailResource**: Send email notifications via SMTP

### Asset Checks

The platform includes comprehensive data quality validation:

#### Data Quality Checks
- **check_no_nulls**: Verify columns have no null values
- **check_unique_values**: Ensure values are unique in specified columns
- **check_row_count**: Validate row count within expected range
- **check_column_values**: Verify column values are within allowed set
- **check_numeric_range**: Validate numeric values within range
- **check_date_range**: Validate date values within range

#### Schema Checks
- **check_required_columns**: Verify all required columns are present
- **check_column_types**: Validate column data types
- **check_no_extra_columns**: Ensure no unexpected columns exist
- **check_column_order**: Verify column order matches expectation
- **validate_schema**: Comprehensive schema validation

#### Freshness Checks
- **check_data_freshness**: Verify data is within freshness window
- **check_update_frequency**: Validate data update frequency
- **check_business_hours_data**: Analyze data distribution by time
- **check_data_continuity**: Check for gaps in data timeline

## SDK

### Decorators

The SDK provides decorators for common asset patterns:

#### Asset Decorators
- **@bronze_asset**: Decorator for raw data ingestion assets
- **@silver_asset**: Decorator for cleaned/validated assets
- **@gold_asset**: Decorator for business logic assets
- **@cost_tracked_asset**: Decorator for cost tracking
- **@schema_validated_asset**: Decorator for schema validation
- **@freshness_monitored_asset**: Decorator for freshness monitoring

#### Compute Decorators
- **@lambda_compute**: Execute on AWS Lambda
- **@fargate_compute**: Execute on AWS Fargate
- **@eks_compute**: Execute on Amazon EKS
- **@batch_compute**: Execute on AWS Batch
- **@cost_optimized_compute**: Cost-optimized execution

### Factories

Factory functions for creating common asset patterns:

#### Asset Factories
- **create_ingestion_asset**: Create data ingestion assets
- **create_transformation_asset**: Create data transformation assets
- **create_output_asset**: Create data output/export assets
- **create_multi_output_asset**: Create multi-output assets

## Observability

### Hooks

Execution lifecycle hooks for monitoring:

#### Execution Hooks
- **log_success_hook**: Log successful executions
- **log_failure_hook**: Log failed executions
- **cost_tracking_hook**: Track execution costs
- **data_lineage_hook**: Track data lineage
- **retry_hook**: Handle retry logic

#### Notification Hooks
- **slack_failure_hook**: Send Slack notifications on failure
- **slack_success_hook**: Send Slack notifications on success
- **email_failure_hook**: Send email notifications on failure
- **create_pagerduty_hook**: Create PagerDuty alerts

### Sensors

Event-driven processing sensors:

#### File Sensors
- **create_s3_file_sensor**: Monitor S3 for new files
- **create_local_file_sensor**: Monitor local directory for files

#### Schedule Sensors
- **create_failure_recovery_sensor**: Monitor for failures and retry
- **create_sla_monitoring_sensor**: Monitor SLA violations
- **create_dependency_sensor**: Trigger on upstream job completion

## Usage Examples

### Basic Asset Creation

```python
from platform_core.sdk.decorators import bronze_asset, silver_asset, gold_asset
from platform_core.resources import PostgresResource

@bronze_asset(name="raw_data")
def ingest_raw_data(context, database: PostgresResource):
    return database.execute_query("SELECT * FROM source_table")

@silver_asset(name="cleaned_data", data_quality_checks=["no_nulls"])
def clean_data(context, raw_data):
    return raw_data.dropna()

@gold_asset(name="business_metrics", business_owner="analytics_team")
def calculate_metrics(context, cleaned_data):
    return cleaned_data.groupby("category").sum()
```

### Resource Configuration

```python
from platform_core.resources import S3Resource, PostgresResource

resources = {
    "s3": S3Resource(
        bucket_name="my-data-bucket",
        region_name="eu-west-2"
    ),
    "database": PostgresResource(
        host="localhost",
        port=5432,
        database="analytics",
        username="dagster",
        password="password"
    )
}
```

### Quality Checks

```python
from platform_core.asset_checks import check_no_nulls, validate_schema

@asset_check(asset="customer_data")
def validate_customer_data(context, customer_data):
    # Check for null values
    null_check = check_no_nulls(customer_data, ["customer_id", "email"])
    
    # Validate schema
    schema = {
        "required_columns": ["customer_id", "email", "name"],
        "column_types": {
            "customer_id": "integer",
            "email": "string",
            "name": "string"
        }
    }
    schema_check = validate_schema(customer_data, schema)
    
    return null_check and schema_check
```

---

*This documentation will be automatically generated from code docstrings when the platform modules are installed. For the latest API details, refer to the source code in the `dagster-platform` package.*