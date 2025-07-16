# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial project scaffolding with cookiecutter templates
- Dagster platform architecture with core components:
  - Platform core resources and utilities
  - Observability framework with sensors, hooks, alerts, and metrics
  - Deployment infrastructure with Helm, Terraform, and Docker
  - SDK with custom decorators, types, and factories
  - Governance framework with policies, lineage, and compliance tools
- Project template with bronze/silver/gold asset structure
- DBT integration for data transformations
- Comprehensive testing framework
- Configuration management with environment-specific settings
- Cookiecutter template with configurable options:
  - Multiple compute environments (Lambda, Fargate, EKS, Batch)
  - Data warehouse integrations (Postgres, Redshift, BigQuery, Snowflake)
  - Object storage options (S3, GCS, Azure Blob)
  - Compliance frameworks (GDPR, UK Data Protection)
  - Monitoring levels (Basic, Standard, Comprehensive)
- Comprehensive README with architecture overview and quick start guide
- Claude instructions file (CLAUDE.md) for AI-assisted development
- MIT license

### Features

- Asset-centric architecture for better lineage and governance
- Intelligent compute orchestration with cost-conscious workload placement
- Built-in data quality validation with asset checks
- Automatic lineage tracking for data dependencies
- Compliance reporting for regulatory requirements
- PII detection and masking capabilities
- Spot instance integration for cost-effective processing
- Per-job cost attribution and tracking
- Automated resource right-sizing based on historical usage
- Type-safe configuration with Pydantic validation
- Local development environment with Docker Compose

### Documentation

- Detailed platform engineering guide (dagster_platform_guide.md)
- Comprehensive README with examples and usage instructions
- Development setup and contribution guidelines
- Security and compliance documentation

## [0.1.0] - 2025-07-16

### Added

- Initial repository setup
- Basic project structure
- License and initial README