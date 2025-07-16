# Platform Core Resources

The platform core provides pre-configured resources for common data engineering tasks.

## AWS Resources

### S3 Resource
```python
from platform_core.resources.aws_resources import S3Resource

s3 = S3Resource(
    bucket="my-data-bucket",
    region="us-east-1"
)
```

### Secrets Manager Resource
```python
from platform_core.resources.aws_resources import SecretsManagerResource

secrets = SecretsManagerResource(
    region="us-east-1"
)
```

### Lambda Resource
```python
from platform_core.resources.aws_resources import LambdaResource

lambda_resource = LambdaResource(
    region="us-east-1",
    role_arn="arn:aws:iam::123456789012:role/lambda-role"
)
```

## Database Resources

### PostgreSQL Resource
```python
from platform_core.resources.database_resources import PostgreSQLResource

db = PostgreSQLResource(
    host="localhost",
    port=5432,
    database="mydatabase",
    username="user",
    password="password"
)
```

### Redshift Resource
```python
from platform_core.resources.database_resources import RedshiftResource

redshift = RedshiftResource(
    host="redshift-cluster.example.com",
    port=5439,
    database="analytics",
    username="analyst",
    password="password"
)
```

## API Resources

### REST API Resource
```python
from platform_core.resources.api_resources import RestAPIResource

api = RestAPIResource(
    base_url="https://api.example.com",
    api_key="your-api-key",
    timeout=30
)
```

### GraphQL Resource
```python
from platform_core.resources.api_resources import GraphQLResource

graphql = GraphQLResource(
    endpoint="https://graphql.example.com",
    headers={"Authorization": "Bearer token"}
)
```

## Usage Examples

### Basic Asset with Resources
```python
from dagster import asset
from platform_core.resources.aws_resources import S3Resource

@asset
def customer_data(s3: S3Resource):
    """Load customer data from S3."""
    client = s3.get_client()
    response = client.get_object(
        Bucket=s3.bucket,
        Key="raw/customers.json"
    )
    return json.loads(response['Body'].read())
```

### Multiple Resources
```python
@asset
def processed_customer_data(
    s3: S3Resource,
    db: PostgreSQLResource,
    api: RestAPIResource
):
    """Process customer data using multiple resources."""
    # Load from S3
    raw_data = load_from_s3(s3)
    
    # Enrich with API data
    enriched_data = api.make_request("customers/enrich")
    
    # Store in database
    with db.get_connection() as conn:
        store_data(conn, enriched_data)
```

## Resource Factory

The platform provides a factory function to create standard resource sets:

```python
from platform_core.resources import create_aws_resources

resources = create_aws_resources(
    region="us-east-1",
    s3_bucket="my-bucket",
    secrets_prefix="prod/"
)
```

## Environment Configuration

### Development
```python
dev_resources = {
    "s3": S3Resource(
        bucket="dev-data-bucket",
        region="us-west-2"
    ),
    "db": PostgreSQLResource(
        host="localhost",
        database="dev_db"
    )
}
```

### Production
```python
from dagster import EnvVar

prod_resources = {
    "s3": S3Resource(
        bucket=EnvVar("PROD_S3_BUCKET"),
        region=EnvVar("AWS_REGION")
    ),
    "db": PostgreSQLResource(
        host=EnvVar("DB_HOST"),
        database=EnvVar("DB_NAME"),
        username=EnvVar("DB_USER"),
        password=EnvVar("DB_PASSWORD")
    )
}
```

## Custom Resources

Extend the platform resources for specific needs:

```python
from platform_core.resources.base import BaseResource

class CustomAPIResource(BaseResource):
    endpoint: str
    auth_token: str
    
    def make_authenticated_request(self, path: str):
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        return requests.get(f"{self.endpoint}/{path}", headers=headers)
```

## Testing

### Mock Resources
```python
from platform_core.resources.testing import MockS3Resource

test_resources = {
    "s3": MockS3Resource()
}
```

### Resource Validation
```python
def test_s3_resource():
    s3 = S3Resource(
        bucket="test-bucket",
        region="us-east-1"
    )
    
    # Test connection
    client = s3.get_client()
    assert client is not None
```

## Related Documentation

- [Resource Management](../architecture/resource-management.md)
- [Configuration](../reference/configuration.md)
- [Testing](../best-practices/testing.md)