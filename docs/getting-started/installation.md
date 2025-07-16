# Installation

This guide will help you set up your development environment for the Dagster Platform.

## Prerequisites

Before getting started, ensure you have the following installed:

- **Python 3.9+** (Python 3.11 recommended)
- **Git** for version control
- **Docker** for containerized development
- **AWS CLI** configured with appropriate credentials

## Environment Setup

### 1. Python Environment

We recommend using a virtual environment to isolate dependencies:

=== "Using venv"
    
    ```bash
    # Create virtual environment
    python -m venv dagster-platform-env
    
    # Activate environment
    source dagster-platform-env/bin/activate  # Linux/Mac
    # or
    dagster-platform-env\Scripts\activate  # Windows
    ```

=== "Using conda"
    
    ```bash
    # Create conda environment
    conda create -n dagster-platform python=3.11
    
    # Activate environment
    conda activate dagster-platform
    ```

=== "Using poetry"
    
    ```bash
    # Install poetry if not already installed
    curl -sSL https://install.python-poetry.org | python3 -
    
    # Create project with poetry
    poetry new my-dagster-project
    cd my-dagster-project
    poetry shell
    ```

### 2. Install Core Dependencies

Install the platform core package and development tools:

```bash
# Install platform core
pip install -e dagster-platform/

# Install development dependencies
pip install -e "dagster-platform/[dev]"

# Install documentation dependencies
pip install -r docs/requirements.txt
```

### 3. Verify Installation

Test that everything is installed correctly:

```bash
# Check Dagster version
dagster --version

# Check platform imports
python -c "from platform_core.resources import S3Resource; print('Platform core installed successfully')"
```

## Development Tools

### IDE Setup

For the best development experience, we recommend using VS Code with these extensions:

- **Python** - Python language support
- **Dagster** - Dagster-specific syntax highlighting
- **Git Lens** - Enhanced Git capabilities
- **Docker** - Container development support

### Code Quality Tools

The platform includes pre-configured code quality tools:

```bash
# Install pre-commit hooks
pre-commit install

# Run linting
ruff check .
ruff format .

# Run type checking
mypy .

# Run tests
pytest
```

## AWS Configuration

### 1. AWS Credentials

Configure your AWS credentials for local development:

```bash
# Configure AWS CLI
aws configure

# Or export environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-west-2
```

### 2. Required AWS Services

Ensure you have access to the following AWS services:

- **S3** - Object storage for data files
- **Secrets Manager** - Secure credential storage
- **Lambda** - Serverless compute
- **ECS/Fargate** - Container orchestration
- **EKS** - Kubernetes clusters (optional)
- **Batch** - Batch processing (optional)

## Docker Setup

### 1. Docker Compose

The platform includes a Docker Compose configuration for local development:

```bash
# Start local services
docker-compose up -d

# Check services are running
docker-compose ps

# View logs
docker-compose logs -f dagster-webserver
```

### 2. Local Services

The Docker setup includes:

- **Dagster Webserver** - UI at http://localhost:3000
- **Dagster Daemon** - Background job execution
- **PostgreSQL** - Metadata storage
- **Redis** - Caching and queuing

## Database Setup

### PostgreSQL (Local Development)

For local development, use the included PostgreSQL container:

```bash
# Database connection details
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dagster
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster
```

### Production Database

For production environments, configure connection to your managed database:

```yaml
# dagster.yaml
storage:
  postgres:
    postgres_host: your-prod-db-host
    postgres_port: 5432
    postgres_db: dagster_prod
    postgres_user: dagster_user
    postgres_password: 
      env: POSTGRES_PASSWORD
```

## Environment Variables

Create a `.env` file in your project root:

```bash
# Development environment
DAGSTER_HOME=/path/to/your/dagster/home
DAGSTER_ENV=development

# AWS Configuration
AWS_DEFAULT_REGION=eu-west-2
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dagster
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster

# Platform Configuration
PLATFORM_ENV=development
LOG_LEVEL=INFO
```

## Troubleshooting

### Common Issues

??? warning "Import errors for platform_core"
    
    ```bash
    # Ensure platform core is installed in development mode
    pip install -e dagster-platform/
    
    # Check Python path
    python -c "import sys; print(sys.path)"
    ```

??? warning "AWS credential errors"
    
    ```bash
    # Check AWS configuration
    aws sts get-caller-identity
    
    # Verify IAM permissions
    aws iam get-user
    ```

??? warning "Docker connection issues"
    
    ```bash
    # Restart Docker services
    docker-compose down
    docker-compose up -d
    
    # Check port conflicts
    netstat -tlnp | grep 3000
    ```

??? warning "Database connection errors"
    
    ```bash
    # Check PostgreSQL is running
    docker-compose ps postgres
    
    # Connect to database directly
    psql -h localhost -p 5432 -U dagster -d dagster
    ```

### Getting Help

If you encounter issues:

1. Check the [troubleshooting guide](../reference/troubleshooting.md)
2. Review the [FAQ](../reference/faq.md)
3. Look at the [GitHub issues](https://github.com/craig-shenton/dagster_platform_example/issues)

## Next Steps

Now that you have everything installed, let's create your first pipeline:

**[Continue to Quick Start →](quick-start.md)**

## Development Workflow

Once installed, your typical development workflow will be:

1. **Activate environment**: `source dagster-platform-env/bin/activate`
2. **Start services**: `docker-compose up -d`
3. **Develop assets**: Create/modify assets in your project
4. **Test locally**: `dagster dev`
5. **Run quality checks**: `pre-commit run --all-files`
6. **Deploy**: Use CI/CD pipeline for deployment

## Optional Components

### Jupyter Notebooks

For data exploration and prototyping:

```bash
pip install jupyter
jupyter lab
```

### Data Visualization

For creating dashboards and reports:

```bash
pip install plotly streamlit
```

### MLflow

For machine learning model tracking:

```bash
pip install mlflow
```

These tools integrate well with the platform and can be used for advanced analytics workflows.