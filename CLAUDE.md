# Claude Instructions for Dagster Platform

## Project Overview
This repository contains a modern, scalable Dagster platform for UK Civil Service data engineering teams. It consists of:

- **dagster-platform/** - Centralized platform infrastructure and shared utilities
- **dagster-project-template/** - Cookiecutter template for new data pipeline projects

## Architecture Principles
- Asset-centric architecture with Dagster assets for lineage and governance
- Separation of concerns between platform and project teams
- Configuration as code with version-controlled YAML
- Cost-conscious compute with intelligent workload placement
- Security by design with built-in compliance tooling

## Development Commands

### Testing
```bash
# Run platform tests
cd dagster-platform && python -m pytest tests/

# Run project template tests
cd dagster-project-template && python -m pytest tests/
```

### Linting and Type Checking
```bash
# Run linting
ruff check .
ruff format .

# Type checking
mypy .
```

### Project Generation
```bash
# Generate new project from template
cookiecutter dagster-project-template/
```

## Key Components

### Platform Core (`dagster-platform/platform_core/`)
- **resources/** - Shared Dagster resources (DB, S3, APIs)
- **asset_checks/** - Reusable data quality validations
- **partitions/** - Common partitioning strategies
- **compute_kinds/** - Compute type definitions
- **io_managers/** - Standardized data I/O patterns

### Observability (`dagster-platform/observability/`)
- **sensors/** - Platform-wide monitoring
- **hooks/** - Execution lifecycle hooks
- **alerts/** - Alerting configurations
- **metrics/** - Custom metrics collection

### Project Template Structure
- **assets/bronze/** - Raw data ingestion
- **assets/silver/** - Data cleaning and validation
- **assets/gold/** - Business logic and outputs
- **dbt/** - DBT integration for transformations
- **tests/** - Comprehensive test suite

## Compute Orchestration
The platform automatically selects compute resources:
- **Lambda** - Stateless validation and triggers
- **Fargate** - Short-lived containerized tasks (<15 minutes)
- **EKS + Karpenter** - Parallel processing with auto-scaling
- **EC2 Spot/Batch** - Long-running or GPU-intensive workloads

## Data Quality & Governance
- Asset checks for automated data quality validation
- Lineage tracking for data dependencies
- Compliance reporting for regulatory requirements
- PII detection and masking capabilities

## Cost Management Features
- Spot instance integration for cost-effective processing
- Idle resource detection
- Per-job cost attribution
- Automated resource right-sizing

## Security & Compliance
- Built-in governance policies
- GDPR and UK Data Protection compliance
- Security by design principles
- PII detection and masking

## When Working on This Repository
1. Follow the asset-centric architecture patterns
2. Maintain separation between platform and project concerns
3. Use the existing testing framework for new features
4. Follow cost-conscious compute patterns
5. Ensure security and compliance requirements are met
6. Update documentation for any new platform features