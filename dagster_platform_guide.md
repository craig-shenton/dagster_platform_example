# Dagster Platform Engineering Guide

## Executive Summary

This guide outlines the creation of a modern, scalable Dagster platform for the UK Civil Service data engineering teams. The platform provides a foundation for building reliable, cost-effective data pipelines whilst maintaining strict governance and security standards.

## Platform Architecture

### Core Components

The platform consists of two primary repositories:

1. **dagster-platform** - Centralised platform infrastructure and shared utilities
2. **dagster-project-template** - Standardised template for new data pipeline projects

### Design Principles

- **Asset-centric architecture** - All data processing built around Dagster assets for better lineage and governance
- **Separation of concerns** - Platform team manages infrastructure, project teams focus on business logic
- **Configuration as code** - All pipeline definitions managed through version-controlled YAML
- **Cost-conscious compute** - Intelligent workload placement across Lambda, Fargate, EKS, and Batch
- **Security by design** - Built-in compliance and governance tooling

## Platform Repository Structure

```
dagster-platform/
├── platform_core/
│   ├── resources/          # Shared Dagster resources (DB, S3, APIs)
│   ├── asset_checks/       # Reusable data quality validations
│   ├── partitions/         # Common partitioning strategies
│   ├── compute_kinds/      # Compute type definitions
│   └── io_managers/        # Standardised data I/O patterns
├── observability/
│   ├── sensors/            # Platform-wide monitoring
│   ├── hooks/              # Execution lifecycle hooks
│   ├── alerts/             # Alerting configurations
│   └── metrics/            # Custom metrics collection
├── deployment/
│   ├── helm/               # Kubernetes deployments
│   ├── terraform/          # Infrastructure as code
│   └── docker/             # Container definitions
├── sdk/
│   ├── decorators/         # Custom asset decorators
│   ├── types/              # Type definitions
│   └── factories/          # Asset factory functions
└── governance/
    ├── policies/           # Data governance policies
    ├── lineage/            # Lineage tracking utilities
    └── compliance/         # Regulatory compliance tools
```

## Project Template Structure

```
dagster-project-template/
├── {{cookiecutter.project_name}}/
│   ├── assets/
│   │   ├── bronze/          # Raw data ingestion assets
│   │   ├── silver/          # Data cleaning and validation
│   │   └── gold/            # Business logic and outputs
│   ├── asset_checks/        # Project-specific validations
│   ├── jobs/                # Asset materialisation jobs
│   ├── schedules/           # Time-based triggers
│   ├── sensors/             # Event-driven triggers
│   └── resources/           # Project-specific resources
├── dbt/                     # DBT integration for transformations
│   ├── models/
│   ├── tests/
│   └── macros/
├── tests/
│   ├── assets/              # Asset-specific unit tests
│   ├── checks/              # Data quality test suite
│   └── integration/         # End-to-end pipeline tests
├── config/
│   ├── base.yaml            # Base configuration
│   ├── dev.yaml             # Development overrides
│   └── prod.yaml            # Production configuration
└── cookiecutter.json        # Template parameters
```

## Key Features

### Intelligent Compute Orchestration

The platform automatically selects appropriate compute resources based on job characteristics:

- **Lambda** - Stateless validation and trigger functions
- **Fargate** - Short-lived containerised tasks (<15 minutes)
- **EKS + Karpenter** - Parallel processing with auto-scaling
- **EC2 Spot/Batch** - Long-running or GPU-intensive workloads

### Data Quality and Governance

- **Asset checks** provide automated data quality validation
- **Lineage tracking** automatically captures data dependencies
- **Compliance reporting** ensures regulatory requirements are met
- **PII detection** and masking capabilities built-in

### Cost Management

- **Spot instance integration** for cost-effective processing
- **Idle resource detection** prevents unnecessary costs
- **Per-job cost attribution** enables accurate chargeback
- **Automated resource right-sizing** based on historical usage

### Developer Experience

- **Cookiecutter templates** for rapid project setup
- **Type-safe configuration** with Pydantic validation
- **Comprehensive testing** framework with asset-specific tests
- **Local development** environment with Docker Compose
