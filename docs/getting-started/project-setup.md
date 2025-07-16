# Project Setup

This guide walks you through setting up a new Dagster project using the platform template.

## Prerequisites

- Python 3.9+
- Docker
- AWS CLI configured
- Git

## Using the Cookiecutter Template

The platform provides a cookiecutter template to scaffold new data projects:

```bash
cookiecutter https://github.com/your-org/dagster-platform-example
```

You'll be prompted to configure:

- **project_name**: Human-readable project name
- **project_slug**: Python package name (snake_case)
- **environment**: Development environment (dev, staging, prod)
- **aws_region**: AWS region for resources
- **include_bronze_assets**: Include bronze layer assets
- **include_silver_assets**: Include silver layer assets
- **include_gold_assets**: Include gold layer assets

## Project Structure

After running cookiecutter, you'll have:

```
my-dagster-project/
├── my_dagster_project/
│   ├── assets/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── resources/
│   └── definitions.py
├── tests/
├── setup.py
├── pyproject.toml
└── README.md
```

## Configuration

### Environment Variables

Set up your environment variables:

```bash
export DAGSTER_CLOUD_URL="your-cloud-url"
export AWS_REGION="us-east-1"
export DATABASE_URL="postgresql://..."
```

### Resources Configuration

Update the resources in `definitions.py`:

```python
from dagster import Definitions
from platform_core.resources import create_aws_resources

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=create_aws_resources(
        region="us-east-1",
        s3_bucket="my-data-bucket"
    )
)
```

## Next Steps

1. [Run your first pipeline](quick-start.md)
2. [Explore the architecture](../architecture/platform-overview.md)
3. [Learn best practices](../best-practices/asset-development.md)