"""
Custom asset decorators for the Dagster platform.
"""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

from dagster import AssetMaterialization, MetadataValue, asset, multi_asset
from dagster._core.definitions.asset_spec import AssetSpec

from platform_core.asset_checks.data_quality_checks import (
    check_no_nulls,
    check_row_count,
    check_unique_values,
)
from platform_core.asset_checks.schema_checks import validate_schema


def bronze_asset(
    name: Optional[str] = None,
    description: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    **kwargs,
):
    """
    Decorator for bronze (raw ingestion) assets.
    
    Args:
        name: Asset name
        description: Asset description
        metadata: Asset metadata
        tags: Asset tags
        **kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        # Add bronze-specific metadata and tags
        bronze_metadata = {
            "tier": "bronze",
            "stage": "raw_ingestion",
            **(metadata or {}),
        }
        
        bronze_tags = {
            "tier": "bronze",
            "stage": "raw_ingestion",
            **(tags or {}),
        }
        
        # Apply asset decorator with bronze-specific configuration
        return asset(
            name=name,
            description=description or f"Bronze asset: {func.__name__}",
            metadata=bronze_metadata,
            tags=bronze_tags,
            **kwargs,
        )(func)
    
    return decorator


def silver_asset(
    name: Optional[str] = None,
    description: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    data_quality_checks: Optional[List[str]] = None,
    **kwargs,
):
    """
    Decorator for silver (cleaned/validated) assets.
    
    Args:
        name: Asset name
        description: Asset description
        metadata: Asset metadata
        tags: Asset tags
        data_quality_checks: List of data quality checks to apply
        **kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Apply data quality checks if specified
            if data_quality_checks and hasattr(result, 'columns'):  # Check if it's a DataFrame
                context = args[0] if args else None
                
                for check_name in data_quality_checks:
                    if check_name == "no_nulls":
                        check_result = check_no_nulls(result, result.columns.tolist())
                        if context and not check_result.passed:
                            context.log.warning(f"Data quality check failed: {check_result.description}")
                    
                    elif check_name == "row_count":
                        check_result = check_row_count(result)
                        if context and not check_result.passed:
                            context.log.warning(f"Data quality check failed: {check_result.description}")
                    
                    elif check_name == "unique_values":
                        # This would need to be configured per asset
                        pass
            
            return result
        
        # Add silver-specific metadata and tags
        silver_metadata = {
            "tier": "silver",
            "stage": "cleaned_validated",
            "data_quality_checks": data_quality_checks or [],
            **(metadata or {}),
        }
        
        silver_tags = {
            "tier": "silver",
            "stage": "cleaned_validated",
            **(tags or {}),
        }
        
        # Apply asset decorator with silver-specific configuration
        return asset(
            name=name,
            description=description or f"Silver asset: {func.__name__}",
            metadata=silver_metadata,
            tags=silver_tags,
            **kwargs,
        )(wrapper)
    
    return decorator


def gold_asset(
    name: Optional[str] = None,
    description: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    business_owner: Optional[str] = None,
    **kwargs,
):
    """
    Decorator for gold (business logic) assets.
    
    Args:
        name: Asset name
        description: Asset description
        metadata: Asset metadata
        tags: Asset tags
        business_owner: Business owner of the asset
        **kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        # Add gold-specific metadata and tags
        gold_metadata = {
            "tier": "gold",
            "stage": "business_logic",
            "business_owner": business_owner,
            **(metadata or {}),
        }
        
        gold_tags = {
            "tier": "gold",
            "stage": "business_logic",
            **(tags or {}),
        }
        
        if business_owner:
            gold_tags["business_owner"] = business_owner
        
        # Apply asset decorator with gold-specific configuration
        return asset(
            name=name,
            description=description or f"Gold asset: {func.__name__}",
            metadata=gold_metadata,
            tags=gold_tags,
            **kwargs,
        )(func)
    
    return decorator


def cost_tracked_asset(
    compute_kind: str = "python",
    estimated_cost_per_run: Optional[float] = None,
    **asset_kwargs,
):
    """
    Decorator for assets with cost tracking capabilities.
    
    Args:
        compute_kind: Type of compute used
        estimated_cost_per_run: Estimated cost per run in USD
        **asset_kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(context, *args, **kwargs):
            import time
            start_time = time.time()
            
            # Execute the original function
            result = func(context, *args, **kwargs)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Log cost tracking information
            context.log.info(
                f"Cost tracking: {func.__name__} executed in {execution_time:.2f}s",
                extra={
                    "execution_time": execution_time,
                    "compute_kind": compute_kind,
                    "estimated_cost": estimated_cost_per_run,
                },
            )
            
            # Add cost metadata to materialization
            if hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "execution_time_seconds": MetadataValue.float(execution_time),
                    "compute_kind": MetadataValue.text(compute_kind),
                    "estimated_cost_usd": MetadataValue.float(estimated_cost_per_run) if estimated_cost_per_run else None,
                })
            
            return result
        
        # Add cost tracking metadata and tags
        cost_metadata = {
            "compute_kind": compute_kind,
            "estimated_cost_per_run": estimated_cost_per_run,
            **(asset_kwargs.get("metadata", {})),
        }
        
        cost_tags = {
            "compute_kind": compute_kind,
            "cost_tracked": "true",
            **(asset_kwargs.get("tags", {})),
        }
        
        asset_kwargs["metadata"] = cost_metadata
        asset_kwargs["tags"] = cost_tags
        
        # Apply asset decorator with cost tracking
        return asset(**asset_kwargs)(wrapper)
    
    return decorator


def schema_validated_asset(
    schema: Dict[str, Any],
    **asset_kwargs,
):
    """
    Decorator for assets with schema validation.
    
    Args:
        schema: Expected schema definition
        **asset_kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(context, *args, **kwargs):
            # Execute the original function
            result = func(context, *args, **kwargs)
            
            # Validate schema if result is a DataFrame
            if hasattr(result, 'columns'):
                schema_check = validate_schema(result, schema)
                
                if not schema_check.passed:
                    context.log.error(f"Schema validation failed: {schema_check.description}")
                    raise ValueError(f"Schema validation failed: {schema_check.description}")
                else:
                    context.log.info("Schema validation passed")
            
            return result
        
        # Add schema validation metadata
        schema_metadata = {
            "schema_validation": True,
            "expected_schema": schema,
            **(asset_kwargs.get("metadata", {})),
        }
        
        schema_tags = {
            "schema_validated": "true",
            **(asset_kwargs.get("tags", {})),
        }
        
        asset_kwargs["metadata"] = schema_metadata
        asset_kwargs["tags"] = schema_tags
        
        # Apply asset decorator with schema validation
        return asset(**asset_kwargs)(wrapper)
    
    return decorator


def freshness_monitored_asset(
    freshness_policy_minutes: int = 60,
    **asset_kwargs,
):
    """
    Decorator for assets with freshness monitoring.
    
    Args:
        freshness_policy_minutes: Expected freshness in minutes
        **asset_kwargs: Additional asset parameters
    
    Returns:
        Decorated asset function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(context, *args, **kwargs):
            # Execute the original function
            result = func(context, *args, **kwargs)
            
            # Add freshness metadata
            if hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "freshness_policy_minutes": MetadataValue.int(freshness_policy_minutes),
                    "materialization_time": MetadataValue.timestamp(context.partition_time_window.start if context.partition_time_window else None),
                })
            
            return result
        
        # Add freshness monitoring metadata
        freshness_metadata = {
            "freshness_policy_minutes": freshness_policy_minutes,
            "freshness_monitored": True,
            **(asset_kwargs.get("metadata", {})),
        }
        
        freshness_tags = {
            "freshness_monitored": "true",
            **(asset_kwargs.get("tags", {})),
        }
        
        asset_kwargs["metadata"] = freshness_metadata
        asset_kwargs["tags"] = freshness_tags
        
        # Apply asset decorator with freshness monitoring
        return asset(**asset_kwargs)(wrapper)
    
    return decorator