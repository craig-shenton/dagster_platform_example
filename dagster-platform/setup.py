from setuptools import setup, find_packages

setup(
    name="dagster-platform",
    version="0.1.0",
    description="Core platform infrastructure and shared utilities for Dagster data pipelines",
    author="Data Engineering Team",
    author_email="data-engineering@example.gov.uk",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "dagster>=1.5.0",
        "dagster-aws>=0.21.0",
        "dagster-dbt>=0.21.0",
        "dagster-postgres>=0.21.0",
        "pydantic>=2.0.0",
        "boto3>=1.28.0",
        "pandas>=2.0.0",
        "great-expectations>=0.18.0",
        "sqlalchemy>=2.0.0",
        "pyyaml>=6.0",
        "requests>=2.31.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
            "mypy>=1.5.0",
            "pre-commit>=3.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)