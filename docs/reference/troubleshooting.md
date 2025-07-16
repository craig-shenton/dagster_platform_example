# Troubleshooting

This guide helps you resolve common issues when working with the Dagster Platform.

## Common Issues

### Installation Problems

#### Import Errors

??? error "ModuleNotFoundError: No module named 'platform_core'"
    
    **Cause**: The platform core package isn't installed or not in the Python path.
    
    **Solution**:
    ```bash
    # Install platform core in development mode
    pip install -e dagster-platform/
    
    # Verify installation
    python -c "import platform_core; print('Success')"
    ```

??? error "ImportError: cannot import name 'X' from 'dagster'"
    
    **Cause**: Version mismatch between Dagster and the platform.
    
    **Solution**:
    ```bash
    # Check Dagster version
    pip show dagster
    
    # Update to required version
    pip install dagster>=1.5.0
    ```

#### Dependency Conflicts

??? error "pip install conflicts or version incompatibilities"
    
    **Cause**: Conflicting package versions in your environment.
    
    **Solution**:
    ```bash
    # Create a fresh virtual environment
    python -m venv fresh-env
    source fresh-env/bin/activate
    
    # Install platform dependencies
    pip install -r dagster-platform/requirements.txt
    ```

### Asset Execution Issues

#### Asset Won't Materialize

??? error "Asset fails to materialize with no clear error"
    
    **Debugging Steps**:
    
    1. Check the asset definition for syntax errors
    2. Verify all upstream dependencies are available
    3. Review the Dagster logs in the UI
    4. Test the asset function directly in Python
    
    ```python
    # Test asset function directly
    from your_project.assets import your_asset
    
    # Mock the context if needed
    class MockContext:
        def __init__(self):
            self.log = print
    
    result = your_asset(MockContext(), upstream_data)
    ```

??? error "DagsterInvalidDefinitionError: Asset dependencies not found"
    
    **Cause**: Asset dependencies are not properly defined or imported.
    
    **Solution**:
    ```python
    # Ensure all dependencies are imported
    from .bronze_assets import raw_data
    from .silver_assets import cleaned_data
    
    @asset(deps=[raw_data, cleaned_data])
    def gold_asset(context, raw_data, cleaned_data):
        return process_data(raw_data, cleaned_data)
    ```

#### Data Quality Check Failures

??? error "Asset check failures causing pipeline to stop"
    
    **Investigation**:
    
    1. Review the specific check that failed
    2. Examine the data that caused the failure
    3. Determine if it's a data issue or check configuration
    
    ```python
    # Debug quality check
    from platform_core.asset_checks import check_no_nulls
    
    # Test check manually
    result = check_no_nulls(your_dataframe, ['column1', 'column2'])
    print(result.description)
    print(result.metadata)
    ```

### Resource Connection Issues

#### Database Connection Problems

??? error "Database connection timeout or authentication failure"
    
    **Common Causes**:
    - Incorrect connection parameters
    - Network connectivity issues
    - Authentication problems
    - Database server down
    
    **Debugging**:
    ```python
    # Test database connection directly
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host="your-host",
            port=5432,
            database="your-db",
            user="your-user",
            password="your-password"
        )
        print("Connection successful")
    except Exception as e:
        print(f"Connection failed: {e}")
    ```

??? error "SSL/TLS connection issues"
    
    **Solution**:
    ```python
    # Configure SSL in resource
    database_resource = PostgresResource(
        host="your-host",
        port=5432,
        database="your-db",
        username="your-user",
        password="your-password",
        connect_args={"sslmode": "require"}
    )
    ```

#### AWS Resource Issues

??? error "AWS credentials not found or invalid"
    
    **Solution**:
    ```bash
    # Check AWS credentials
    aws sts get-caller-identity
    
    # Configure credentials
    aws configure
    
    # Or use environment variables
    export AWS_ACCESS_KEY_ID=your_access_key
    export AWS_SECRET_ACCESS_KEY=your_secret_key
    export AWS_DEFAULT_REGION=eu-west-2
    ```

??? error "S3 bucket access denied"
    
    **Cause**: Insufficient IAM permissions.
    
    **Required IAM permissions**:
    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::your-bucket/*",
                    "arn:aws:s3:::your-bucket"
                ]
            }
        ]
    }
    ```

### Performance Issues

#### Slow Asset Execution

??? warning "Assets taking too long to execute"
    
    **Optimization Strategies**:
    
    1. **Check compute allocation**:
    ```python
    # Increase compute resources
    @fargate_compute(cpu_units=1024, memory_mb=2048)
    def slow_asset(context, data):
        return process_large_data(data)
    ```
    
    2. **Use partitioning**:
    ```python
    # Partition large datasets
    @asset(partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"))
    def partitioned_asset(context, data):
        partition_date = context.asset_partition_key_for_output()
        return process_partition(data, partition_date)
    ```
    
    3. **Optimize data processing**:
    ```python
    # Use efficient pandas operations
    import pandas as pd
    
    # Avoid loops, use vectorized operations
    df['new_column'] = df['col1'] * df['col2']  # Good
    # df['new_column'] = df.apply(lambda x: x['col1'] * x['col2'], axis=1)  # Slow
    ```

#### Memory Issues

??? error "Out of memory errors during execution"
    
    **Solutions**:
    
    1. **Increase memory allocation**:
    ```python
    @fargate_compute(memory_mb=4096)  # Increase memory
    def memory_intensive_asset(context, data):
        return process_large_dataset(data)
    ```
    
    2. **Process data in chunks**:
    ```python
    def process_in_chunks(df, chunk_size=10000):
        for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
            yield process_chunk(chunk)
    ```
    
    3. **Use appropriate data types**:
    ```python
    # Optimize data types
    df['int_column'] = df['int_column'].astype('int32')
    df['category_column'] = df['category_column'].astype('category')
    ```

### Scheduling and Sensor Issues

#### Schedules Not Triggering

??? error "Scheduled jobs not running as expected"
    
    **Check List**:
    
    1. Verify the schedule is turned on in the UI
    2. Check the cron expression is correct
    3. Ensure the Dagster daemon is running
    4. Review the schedule logs
    
    ```python
    # Test cron expression
    from dagster import schedule_from_partitions
    from croniter import croniter
    
    # Verify next run time
    cron = croniter('0 6 * * *')  # 6 AM daily
    print(f"Next run: {cron.get_next()}")
    ```

#### Sensor Not Detecting Events

??? error "File sensor not triggering on new files"
    
    **Debugging**:
    
    1. Check sensor is turned on
    2. Verify file path and permissions
    3. Test sensor logic manually
    
    ```python
    # Test sensor logic
    from your_project.sensors import your_file_sensor
    
    # Mock sensor context
    class MockSensorContext:
        def __init__(self):
            self.cursor = None
            self.log = print
    
    result = your_file_sensor(MockSensorContext())
    print(result)
    ```

### Deployment Issues

#### Docker Build Failures

??? error "Docker build fails with dependency errors"
    
    **Common Solutions**:
    
    ```dockerfile
    # Use multi-stage builds
    FROM python:3.11-slim as builder
    
    # Install system dependencies
    RUN apt-get update && apt-get install -y \
        gcc \
        g++ \
        && rm -rf /var/lib/apt/lists/*
    
    # Install Python dependencies
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt
    
    # Final stage
    FROM python:3.11-slim
    COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
    ```

#### Kubernetes Deployment Issues

??? error "Pods failing to start or crashing"
    
    **Investigation Steps**:
    
    ```bash
    # Check pod status
    kubectl get pods
    
    # View pod logs
    kubectl logs <pod-name>
    
    # Describe pod for events
    kubectl describe pod <pod-name>
    
    # Check resource limits
    kubectl top pod <pod-name>
    ```

### Data Quality Issues

#### False Positive Quality Checks

??? warning "Quality checks failing on valid data"
    
    **Solution**:
    
    1. **Adjust check thresholds**:
    ```python
    # Allow some null values
    @asset_check(asset="customer_data")
    def check_null_threshold(context, customer_data):
        null_percentage = customer_data.isnull().sum().sum() / len(customer_data)
        return AssetCheckResult(
            passed=null_percentage < 0.05,  # Allow 5% nulls
            description=f"Null percentage: {null_percentage:.2%}"
        )
    ```
    
    2. **Update business rules**:
    ```python
    # More flexible validation
    def validate_email_format(email_series):
        # Allow empty emails but validate non-empty ones
        non_empty_emails = email_series.dropna()
        if len(non_empty_emails) == 0:
            return True
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return non_empty_emails.str.match(pattern).all()
    ```

## Getting Help

### Debug Mode

Enable debug logging for more detailed information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or set environment variable
export DAGSTER_LOG_LEVEL=DEBUG
```

### Useful Commands

```bash
# Check Dagster status
dagster instance info

# Validate pipeline definitions
dagster pipeline validate

# Reset asset materialization
dagster asset wipe <asset_name>

# Check asset lineage
dagster asset lineage <asset_name>
```

### Support Resources

1. **Platform Documentation**: This comprehensive guide
2. **Dagster Documentation**: [https://docs.dagster.io/](https://docs.dagster.io/)
3. **GitHub Issues**: Report bugs and feature requests
4. **Stack Overflow**: Community support with `dagster` tag

### Creating Bug Reports

When reporting issues, include:

1. **Error message** and full stack trace
2. **Platform version** and Python version
3. **Minimal code example** that reproduces the issue
4. **Environment details** (OS, container, cloud provider)
5. **Steps to reproduce** the problem

Example bug report template:

```markdown
## Bug Description
Brief description of the issue

## Environment
- Platform version: X.X.X
- Python version: 3.11
- OS: Ubuntu 20.04
- Deployment: Docker/Kubernetes

## Reproduction Steps
1. Step 1
2. Step 2
3. Step 3

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Error Message
```
Full error message and stack trace
```

## Code Example
```python
# Minimal code that reproduces the issue
```
```

This approach helps maintainers quickly understand and resolve issues.