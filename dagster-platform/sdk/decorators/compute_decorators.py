"""
Compute-specific decorators for the Dagster platform.
"""

from functools import wraps
from typing import Callable

from dagster import MetadataValue


def lambda_compute(
    timeout_seconds: int = 900,
    memory_mb: int = 128,
    **decorator_kwargs,
):
    """
    Decorator for Lambda compute workloads.
    
    Args:
        timeout_seconds: Lambda timeout in seconds
        memory_mb: Lambda memory in MB
        **decorator_kwargs: Additional decorator parameters
    
    Returns:
        Decorated function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = args[0] if args else None
            
            # Add Lambda-specific logging
            if context and hasattr(context, 'log'):
                context.log.info(
                    f"Executing {func.__name__} on Lambda compute",
                    extra={
                        "compute_kind": "lambda",
                        "timeout_seconds": timeout_seconds,
                        "memory_mb": memory_mb,
                    },
                )
            
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Add Lambda metadata
            if context and hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "compute_kind": MetadataValue.text("lambda"),
                    "timeout_seconds": MetadataValue.int(timeout_seconds),
                    "memory_mb": MetadataValue.int(memory_mb),
                })
            
            return result
        
        # Add Lambda-specific metadata and tags
        lambda_metadata = {
            "compute_kind": "lambda",
            "timeout_seconds": timeout_seconds,
            "memory_mb": memory_mb,
            **(decorator_kwargs.get("metadata", {})),
        }
        
        lambda_tags = {
            "compute_kind": "lambda",
            "serverless": "true",
            **(decorator_kwargs.get("tags", {})),
        }
        
        decorator_kwargs["metadata"] = lambda_metadata
        decorator_kwargs["tags"] = lambda_tags
        
        return wrapper
    
    return decorator


def fargate_compute(
    cpu_units: int = 256,
    memory_mb: int = 512,
    **decorator_kwargs,
):
    """
    Decorator for Fargate compute workloads.
    
    Args:
        cpu_units: CPU units (256 = 0.25 vCPU)
        memory_mb: Memory in MB
        **decorator_kwargs: Additional decorator parameters
    
    Returns:
        Decorated function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = args[0] if args else None
            
            # Add Fargate-specific logging
            if context and hasattr(context, 'log'):
                context.log.info(
                    f"Executing {func.__name__} on Fargate compute",
                    extra={
                        "compute_kind": "fargate",
                        "cpu_units": cpu_units,
                        "memory_mb": memory_mb,
                    },
                )
            
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Add Fargate metadata
            if context and hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "compute_kind": MetadataValue.text("fargate"),
                    "cpu_units": MetadataValue.int(cpu_units),
                    "memory_mb": MetadataValue.int(memory_mb),
                })
            
            return result
        
        # Add Fargate-specific metadata and tags
        fargate_metadata = {
            "compute_kind": "fargate",
            "cpu_units": cpu_units,
            "memory_mb": memory_mb,
            **(decorator_kwargs.get("metadata", {})),
        }
        
        fargate_tags = {
            "compute_kind": "fargate",
            "containerized": "true",
            **(decorator_kwargs.get("tags", {})),
        }
        
        decorator_kwargs["metadata"] = fargate_metadata
        decorator_kwargs["tags"] = fargate_tags
        
        return wrapper
    
    return decorator


def eks_compute(
    node_type: str = "m5.large",
    min_nodes: int = 1,
    max_nodes: int = 10,
    **decorator_kwargs,
):
    """
    Decorator for EKS compute workloads.
    
    Args:
        node_type: EKS node instance type
        min_nodes: Minimum number of nodes
        max_nodes: Maximum number of nodes
        **decorator_kwargs: Additional decorator parameters
    
    Returns:
        Decorated function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = args[0] if args else None
            
            # Add EKS-specific logging
            if context and hasattr(context, 'log'):
                context.log.info(
                    f"Executing {func.__name__} on EKS compute",
                    extra={
                        "compute_kind": "eks",
                        "node_type": node_type,
                        "min_nodes": min_nodes,
                        "max_nodes": max_nodes,
                    },
                )
            
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Add EKS metadata
            if context and hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "compute_kind": MetadataValue.text("eks"),
                    "node_type": MetadataValue.text(node_type),
                    "min_nodes": MetadataValue.int(min_nodes),
                    "max_nodes": MetadataValue.int(max_nodes),
                })
            
            return result
        
        # Add EKS-specific metadata and tags
        eks_metadata = {
            "compute_kind": "eks",
            "node_type": node_type,
            "min_nodes": min_nodes,
            "max_nodes": max_nodes,
            **(decorator_kwargs.get("metadata", {})),
        }
        
        eks_tags = {
            "compute_kind": "eks",
            "kubernetes": "true",
            "auto_scaling": "true",
            **(decorator_kwargs.get("tags", {})),
        }
        
        decorator_kwargs["metadata"] = eks_metadata
        decorator_kwargs["tags"] = eks_tags
        
        return wrapper
    
    return decorator


def batch_compute(
    instance_type: str = "m5.large",
    spot_fleet: bool = True,
    max_vcpus: int = 256,
    **decorator_kwargs,
):
    """
    Decorator for AWS Batch compute workloads.
    
    Args:
        instance_type: EC2 instance type
        spot_fleet: Whether to use spot instances
        max_vcpus: Maximum vCPUs for the compute environment
        **decorator_kwargs: Additional decorator parameters
    
    Returns:
        Decorated function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = args[0] if args else None
            
            # Add Batch-specific logging
            if context and hasattr(context, 'log'):
                context.log.info(
                    f"Executing {func.__name__} on AWS Batch compute",
                    extra={
                        "compute_kind": "batch",
                        "instance_type": instance_type,
                        "spot_fleet": spot_fleet,
                        "max_vcpus": max_vcpus,
                    },
                )
            
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Add Batch metadata
            if context and hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "compute_kind": MetadataValue.text("batch"),
                    "instance_type": MetadataValue.text(instance_type),
                    "spot_fleet": MetadataValue.bool(spot_fleet),
                    "max_vcpus": MetadataValue.int(max_vcpus),
                })
            
            return result
        
        # Add Batch-specific metadata and tags
        batch_metadata = {
            "compute_kind": "batch",
            "instance_type": instance_type,
            "spot_fleet": spot_fleet,
            "max_vcpus": max_vcpus,
            **(decorator_kwargs.get("metadata", {})),
        }
        
        batch_tags = {
            "compute_kind": "batch",
            "batch_processing": "true",
            **(decorator_kwargs.get("tags", {})),
        }
        
        if spot_fleet:
            batch_tags["spot_instances"] = "true"
        
        decorator_kwargs["metadata"] = batch_metadata
        decorator_kwargs["tags"] = batch_tags
        
        return wrapper
    
    return decorator


def cost_optimized_compute(
    prefer_spot: bool = True,
    max_cost_per_hour: float = 1.0,
    **decorator_kwargs,
):
    """
    Decorator for cost-optimized compute workloads.
    
    Args:
        prefer_spot: Whether to prefer spot instances
        max_cost_per_hour: Maximum cost per hour in USD
        **decorator_kwargs: Additional decorator parameters
    
    Returns:
        Decorated function
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            context = args[0] if args else None
            
            # Add cost optimization logging
            if context and hasattr(context, 'log'):
                context.log.info(
                    f"Executing {func.__name__} with cost optimization",
                    extra={
                        "cost_optimized": True,
                        "prefer_spot": prefer_spot,
                        "max_cost_per_hour": max_cost_per_hour,
                    },
                )
            
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Add cost optimization metadata
            if context and hasattr(context, 'add_output_metadata'):
                context.add_output_metadata({
                    "cost_optimized": MetadataValue.bool(True),
                    "prefer_spot": MetadataValue.bool(prefer_spot),
                    "max_cost_per_hour": MetadataValue.float(max_cost_per_hour),
                })
            
            return result
        
        # Add cost optimization metadata and tags
        cost_metadata = {
            "cost_optimized": True,
            "prefer_spot": prefer_spot,
            "max_cost_per_hour": max_cost_per_hour,
            **(decorator_kwargs.get("metadata", {})),
        }
        
        cost_tags = {
            "cost_optimized": "true",
            **(decorator_kwargs.get("tags", {})),
        }
        
        if prefer_spot:
            cost_tags["spot_preferred"] = "true"
        
        decorator_kwargs["metadata"] = cost_metadata
        decorator_kwargs["tags"] = cost_tags
        
        return wrapper
    
    return decorator