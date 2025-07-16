# Dagster Platform Example

A modern, scalable Dagster platform for UK Civil Service data engineering teams. This repository provides a foundation for building reliable, cost-effective data pipelines with strict governance and security standards.

## 🏗️ Architecture

The platform consists of two primary components:

- **`dagster-platform/`** - Centralized platform infrastructure and shared utilities
- **`dagster-project-template/`** - Cookiecutter template for standardized data pipeline projects

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker (for local development)
- Cookiecutter

### Installation

```bash
# Install cookiecutter if not already installed
pip install cookiecutter

# Generate a new project from the template
cookiecutter dagster-project-template/
```

### Project Structure

```text
dagster-platform/
├── platform_core/          # Shared resources and utilities
├── observability/           # Monitoring and alerting
├── deployment/             # Infrastructure as code
├── sdk/                   # Custom decorators and factories
└── governance/            # Policies and compliance tools

dagster-project-template/
├── {{cookiecutter.project_name}}/
│   ├── assets/
│   │   ├── bronze/        # Raw data ingestion
│   │   ├── silver/        # Data cleaning and validation
│   │   └── gold/          # Business logic and outputs
│   ├── dbt/              # DBT transformations
│   ├── tests/            # Test suite
│   └── config/           # Environment configurations
└── cookiecutter.json     # Template parameters
```

## 🔧 Key Features

### Intelligent Compute Orchestration

- **Lambda** - Stateless validation and triggers
- **Fargate** - Short-lived containerized tasks
- **EKS + Karpenter** - Parallel processing with auto-scaling
- **EC2 Spot/Batch** - Long-running or GPU-intensive workloads

### Data Quality & Governance

- Automated data quality validation with asset checks
- Lineage tracking for data dependencies
- Compliance reporting for regulatory requirements
- Built-in PII detection and masking

### Cost Management

- Spot instance integration for cost-effective processing
- Idle resource detection
- Per-job cost attribution
- Automated resource right-sizing

### Developer Experience

- Cookiecutter templates for rapid project setup
- Type-safe configuration with Pydantic validation
- Comprehensive testing framework
- Local development with Docker Compose

## 📖 Documentation

For detailed implementation guidance, see [dagster_platform_guide.md](dagster_platform_guide.md)

## 🧪 Development

### Testing

```bash
# Run platform tests
cd dagster-platform && python -m pytest tests/

# Run project template tests
cd dagster-project-template && python -m pytest tests/
```

### Linting

```bash
ruff check .
ruff format .
mypy .
```

## 🔐 Security & Compliance

- Security by design principles
- GDPR and UK Data Protection compliance
- Built-in governance policies
- PII detection and masking capabilities

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## 📞 Support

For questions or issues, please open an issue in this repository.