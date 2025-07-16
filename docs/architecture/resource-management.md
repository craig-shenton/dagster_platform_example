# Resource Management

Dagster's resource system provides a flexible way to manage external dependencies and configuration.

## Resource Architecture

### Resource Types

#### Database Resources
```python
from dagster import ConfigurableResource
import psycopg2

class PostgreSQLResource(ConfigurableResource):
    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password
        )
```

#### Cloud Resources
```python
from dagster import ConfigurableResource
import boto3

class S3Resource(ConfigurableResource):
    region: str = "us-east-1"
    bucket: str
    access_key_id: str
    secret_access_key: str
    
    def get_client(self):
        return boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
```

#### API Resources
```python
class APIResource(ConfigurableResource):
    base_url: str
    api_key: str
    timeout: int = 30
    
    def make_request(self, endpoint: str, **kwargs):
        import requests
        return requests.get(
            f"{self.base_url}/{endpoint}",
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout,
            **kwargs
        )
```

## Resource Configuration

### Environment-based Configuration
```python
from dagster import Definitions, EnvVar

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "database": PostgreSQLResource(
            host=EnvVar("DB_HOST"),
            database=EnvVar("DB_NAME"),
            username=EnvVar("DB_USER"),
            password=EnvVar("DB_PASSWORD")
        ),
        "s3": S3Resource(
            bucket=EnvVar("S3_BUCKET"),
            access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY")
        )
    }
)
```

### Multi-environment Configuration
```python
def get_resources_for_env(env: str):
    if env == "prod":
        return {
            "database": PostgreSQLResource(
                host="prod-db.example.com",
                database="production",
                username=EnvVar("PROD_DB_USER"),
                password=EnvVar("PROD_DB_PASSWORD")
            )
        }
    elif env == "dev":
        return {
            "database": PostgreSQLResource(
                host="localhost",
                database="development",
                username="dev_user",
                password="dev_password"
            )
        }
```

## Resource Usage in Assets

### Basic Usage
```python
@asset
def customer_data(database: PostgreSQLResource):
    with database.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM customers")
        return cursor.fetchall()
```

### Multiple Resources
```python
@asset
def processed_data(
    database: PostgreSQLResource,
    s3: S3Resource,
    api: APIResource
):
    # Fetch from database
    with database.get_connection() as conn:
        data = fetch_data(conn)
    
    # Enrich with API data
    enriched_data = api.make_request("enrichment")
    
    # Store in S3
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket=s3.bucket,
        Key="processed/data.json",
        Body=json.dumps(enriched_data)
    )
```

## Resource Lifecycle

### Initialization
```python
class DatabaseResource(ConfigurableResource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._connection = None
    
    def setup_for_execution(self, context):
        self._connection = self.get_connection()
    
    def teardown_after_execution(self, context):
        if self._connection:
            self._connection.close()
```

### Connection Pooling
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

class PooledDatabaseResource(ConfigurableResource):
    connection_string: str
    pool_size: int = 10
    max_overflow: int = 20
    
    def get_engine(self):
        return create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow
        )
```

## Resource Testing

### Mock Resources
```python
class MockDatabaseResource(ConfigurableResource):
    def get_connection(self):
        return MockConnection()

# Test configuration
test_resources = {
    "database": MockDatabaseResource()
}
```

### Resource Validation
```python
class ValidatedResource(ConfigurableResource):
    endpoint: str
    
    def __post_init__(self):
        if not self.endpoint.startswith("https://"):
            raise ValueError("Endpoint must use HTTPS")
```

## Advanced Patterns

### Resource Factories
```python
def create_cloud_resources(region: str, environment: str):
    return {
        "s3": S3Resource(
            region=region,
            bucket=f"data-{environment}-{region}"
        ),
        "lambda": LambdaResource(
            region=region,
            role_arn=f"arn:aws:iam::123456789012:role/{environment}-lambda-role"
        )
    }
```

### Conditional Resources
```python
from dagster import configured

@configured(S3Resource)
def s3_resource_for_env(config):
    env = config.get("environment", "dev")
    if env == "prod":
        return {
            "region": "us-east-1",
            "bucket": "prod-data-bucket"
        }
    return {
        "region": "us-west-2",
        "bucket": "dev-data-bucket"
    }
```

## Security Best Practices

### Secret Management
```python
from dagster import StringSource

class SecureResource(ConfigurableResource):
    api_key: str = Field(
        description="API key for external service",
        default_factory=lambda: os.getenv("API_KEY")
    )
    
    def __post_init__(self):
        if not self.api_key:
            raise ValueError("API key is required")
```

### IAM Integration
```python
class IAMResource(ConfigurableResource):
    role_arn: str
    
    def assume_role(self):
        sts = boto3.client('sts')
        return sts.assume_role(
            RoleArn=self.role_arn,
            RoleSessionName='dagster-session'
        )
```

## Monitoring & Observability

### Resource Metrics
```python
class MonitoredResource(ConfigurableResource):
    def make_request(self, *args, **kwargs):
        start_time = time.time()
        try:
            result = self._make_request(*args, **kwargs)
            self.log_success_metric(time.time() - start_time)
            return result
        except Exception as e:
            self.log_error_metric(e)
            raise
```

### Health Checks
```python
@sensor(job=health_check_job)
def resource_health_sensor(context):
    for resource_name, resource in resources.items():
        if not resource.health_check():
            yield RunRequest(
                run_config={
                    "resources": {
                        "alerting": {
                            "config": {
                                "message": f"Resource {resource_name} is unhealthy"
                            }
                        }
                    }
                }
            )
```

## Related Documentation

- [Platform Overview](platform-overview.md)
- [Compute Orchestration](compute-orchestration.md)
- [Security](../governance/security.md)
- [Configuration](../reference/configuration.md)