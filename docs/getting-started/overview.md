# Getting Started Overview

Welcome to the Dagster Platform! This guide will help you understand the platform architecture and get your first data pipeline running.

## What is the Dagster Platform?

The Dagster Platform is a comprehensive data engineering solution built on top of Dagster, designed specifically for UK Civil Service data teams. It provides:

- **Standardized patterns** for data pipeline development
- **Intelligent compute orchestration** across AWS services
- **Built-in data quality** and governance features
- **Cost optimization** through smart resource allocation
- **Security and compliance** by design

## Platform Architecture

The platform consists of two main components:

### 1. Platform Core (`dagster-platform`)
A Python package containing shared utilities and infrastructure:

```
dagster-platform/
├── platform_core/       # Core Dagster resources and utilities
├── observability/       # Monitoring and alerting
├── sdk/                # Custom decorators and factories
└── governance/         # Compliance and security tools
```

### 2. Project Template (`dagster-project-template`)
A Cookiecutter template for creating new data pipeline projects:

```
dagster-project-template/
├── {{cookiecutter.project_name}}/
│   ├── assets/         # Bronze/Silver/Gold data assets
│   ├── jobs/           # Pipeline job definitions
│   ├── schedules/      # Time-based triggers
│   └── sensors/        # Event-driven triggers
└── cookiecutter.json   # Template configuration
```

## Data Layer Architecture

The platform uses a **medallion architecture** with three data layers:

### 🥉 Bronze Layer
- **Purpose**: Raw data ingestion
- **Compute**: Lambda functions
- **Data**: Unprocessed, exactly as received
- **Example**: Customer records from operational database

### 🥈 Silver Layer
- **Purpose**: Data cleaning and validation
- **Compute**: Fargate containers
- **Data**: Cleaned, validated, and deduplicated
- **Example**: Standardized customer data with quality checks

### 🥇 Gold Layer
- **Purpose**: Business logic and analytics
- **Compute**: EKS clusters
- **Data**: Aggregated metrics and business KPIs
- **Example**: Customer lifetime value calculations

## Compute Orchestration

The platform automatically selects the most cost-effective compute for each workload:

| Compute Type | Use Case | Duration | Cost |
|-------------|----------|----------|------|
| **Lambda** | Validation, triggers | < 15 min | Lowest |
| **Fargate** | Data processing | < 4 hours | Low |
| **EKS** | Analytics, ML | Any | Medium |
| **Batch** | Heavy processing | > 4 hours | Variable |

## Key Features

### 🔍 Data Quality Framework
- Automated schema validation
- Data freshness monitoring
- Completeness and accuracy checks
- Custom business rule validation

### 📊 Observability
- Real-time pipeline monitoring
- Automatic failure detection and recovery
- Cost tracking and optimization
- Custom metrics and alerts

### 🔒 Security & Compliance
- Built-in PII detection and masking
- GDPR and UK Data Protection compliance
- Comprehensive audit logging
- Role-based access control

## Development Workflow

1. **Generate Project**: Use Cookiecutter to create new pipeline project
2. **Develop Assets**: Create bronze, silver, and gold assets
3. **Add Quality Checks**: Implement data validation rules
4. **Configure Jobs**: Define pipeline execution jobs
5. **Deploy**: Use CI/CD pipeline for automated deployment

## Next Steps

Now that you understand the platform architecture, let's get started:

1. **[Installation](installation.md)** - Set up your development environment
2. **[Quick Start](quick-start.md)** - Create your first pipeline
3. **[Project Setup](project-setup.md)** - Configure a production-ready project

## Common Questions

??? question "Do I need to know Dagster to use this platform?"

    Basic Dagster knowledge is helpful but not required. The platform provides templates and patterns that abstract away much of the complexity. We recommend reading the [Dagster documentation](https://docs.dagster.io/) for deeper understanding.

??? question "Can I customize the compute types?"

    Yes! The platform uses decorators to specify compute types. You can easily switch between Lambda, Fargate, EKS, and Batch based on your workload requirements.

??? question "How do I handle sensitive data?"

    The platform includes built-in PII detection and masking capabilities. All data is encrypted at rest and in transit, and the platform follows UK government security standards.

??? question "What about cost optimization?"

    The platform automatically selects the most cost-effective compute for each workload and provides detailed cost tracking. It also supports spot instances and automatic resource scaling.

## Support

If you need help getting started:

- Check the [troubleshooting guide](../reference/troubleshooting.md)
- Review the [FAQ](../reference/faq.md)
- Look at [example implementations](../best-practices/asset-development.md)

Ready to start building? Let's [install the platform](installation.md)!