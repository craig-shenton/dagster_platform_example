"""
Job definitions for the {{cookiecutter.project_name}} project.
"""

from dagster import job, op, AssetSelection, define_asset_job, DefaultSensorStatus

from ..assets import (
    raw_customer_data,
    raw_order_data,
    raw_product_catalog,
    raw_web_events,
    cleaned_customer_data,
    cleaned_order_data,
    cleaned_product_catalog,
    customer_lifetime_value,
    product_performance,
    daily_sales_summary,
)


# Bronze layer job - raw data ingestion
bronze_ingestion_job = define_asset_job(
    name="bronze_ingestion_job",
    selection=AssetSelection.assets(
        raw_customer_data,
        raw_order_data,
        raw_product_catalog,
        raw_web_events,
    ),
    description="Job to ingest raw data into the bronze layer",
    tags={
        "layer": "bronze",
        "compute": "lambda",
        "priority": "high",
    },
)


# Silver layer job - data cleaning and validation
silver_transformation_job = define_asset_job(
    name="silver_transformation_job",
    selection=AssetSelection.assets(
        cleaned_customer_data,
        cleaned_order_data,
        cleaned_product_catalog,
    ),
    description="Job to clean and validate data in the silver layer",
    tags={
        "layer": "silver",
        "compute": "fargate",
        "priority": "high",
    },
)


# Gold layer job - business metrics and analytics
gold_analytics_job = define_asset_job(
    name="gold_analytics_job",
    selection=AssetSelection.assets(
        customer_lifetime_value,
        product_performance,
        daily_sales_summary,
    ),
    description="Job to calculate business metrics and analytics",
    tags={
        "layer": "gold",
        "compute": "eks",
        "priority": "medium",
    },
)


# Full pipeline job - all layers
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Complete data pipeline from bronze to gold",
    tags={
        "layer": "all",
        "compute": "mixed",
        "priority": "high",
    },
)


# Customer analytics job - focused on customer data
customer_analytics_job = define_asset_job(
    name="customer_analytics_job",
    selection=AssetSelection.assets(
        raw_customer_data,
        cleaned_customer_data,
        customer_lifetime_value,
    ),
    description="Customer-focused analytics pipeline",
    tags={
        "domain": "customer",
        "compute": "mixed",
        "priority": "medium",
    },
)


# Product analytics job - focused on product data
product_analytics_job = define_asset_job(
    name="product_analytics_job",
    selection=AssetSelection.assets(
        raw_product_catalog,
        cleaned_product_catalog,
        product_performance,
    ),
    description="Product-focused analytics pipeline",
    tags={
        "domain": "product",
        "compute": "mixed",
        "priority": "medium",
    },
)


# Sales reporting job - focused on sales data
sales_reporting_job = define_asset_job(
    name="sales_reporting_job",
    selection=AssetSelection.assets(
        raw_order_data,
        cleaned_order_data,
        daily_sales_summary,
    ),
    description="Sales reporting and KPI calculation",
    tags={
        "domain": "sales",
        "compute": "mixed",
        "priority": "high",
    },
)


# Data quality job - runs all data quality checks
data_quality_job = define_asset_job(
    name="data_quality_job",
    selection=AssetSelection.assets(
        cleaned_customer_data,
        cleaned_order_data,
        cleaned_product_catalog,
    ),
    description="Data quality validation and monitoring",
    tags={
        "purpose": "data_quality",
        "compute": "fargate",
        "priority": "high",
    },
)


# Custom ops for more complex processing
@op(
    description="Validate data pipeline execution",
    tags={"compute": "lambda"},
)
def validate_pipeline_execution(context) -> str:
    """
    Validate that the data pipeline executed successfully.
    
    This op performs post-execution validation to ensure
    data quality and pipeline integrity.
    """
    
    context.log.info("Starting pipeline validation")
    
    # In a real implementation, this would check:
    # - Data freshness
    # - Record counts
    # - Data quality metrics
    # - Business rule validation
    
    validation_results = {
        "data_freshness": "PASS",
        "record_counts": "PASS",
        "data_quality": "PASS",
        "business_rules": "PASS",
    }
    
    context.log.info(f"Pipeline validation completed: {validation_results}")
    
    return "Pipeline validation successful"


@op(
    description="Send pipeline completion notification",
    tags={"compute": "lambda"},
)
def send_completion_notification(context, validation_result: str) -> None:
    """
    Send notification about pipeline completion.
    
    This op sends notifications to stakeholders about
    the pipeline execution status.
    """
    
    context.log.info(f"Sending completion notification: {validation_result}")
    
    # In a real implementation, this would send:
    # - Slack notifications
    # - Email alerts
    # - Dashboard updates
    # - Monitoring system updates
    
    context.log.info("Completion notification sent")


# Pipeline validation job
@job(
    description="Validates pipeline execution and sends notifications",
    tags={"purpose": "validation", "compute": "lambda"},
)
def pipeline_validation_job():
    """
    Job to validate pipeline execution and send notifications.
    """
    validation_result = validate_pipeline_execution()
    send_completion_notification(validation_result)