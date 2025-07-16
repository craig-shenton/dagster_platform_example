"""
Execution lifecycle hooks for monitoring and logging.
"""

from typing import Any, Dict, Optional

from dagster import HookContext, failure_hook, success_hook


@success_hook
def log_success_hook(context: HookContext) -> None:
    """Log successful asset/op execution with metadata."""
    
    # Get execution metadata
    metadata = {
        "run_id": context.run_id,
        "asset_key": str(context.asset_key) if context.asset_key else None,
        "op_name": context.op.name if context.op else None,
        "step_key": context.step_key,
        "start_time": context.dagster_run.start_time,
        "end_time": context.dagster_run.end_time,
    }
    
    # Log success with metadata
    context.log.info(
        f"Successfully executed {context.step_key}",
        extra=metadata,
    )


@failure_hook
def log_failure_hook(context: HookContext) -> None:
    """Log failed asset/op execution with error details."""
    
    # Get execution metadata
    metadata = {
        "run_id": context.run_id,
        "asset_key": str(context.asset_key) if context.asset_key else None,
        "op_name": context.op.name if context.op else None,
        "step_key": context.step_key,
        "start_time": context.dagster_run.start_time,
        "failure_time": context.dagster_run.end_time,
        "failure_data": context.failure_data,
    }
    
    # Log failure with metadata
    context.log.error(
        f"Failed to execute {context.step_key}: {context.failure_data}",
        extra=metadata,
    )


@success_hook
def cost_tracking_hook(context: HookContext) -> None:
    """Track execution costs and resource usage."""
    
    # Calculate execution time
    start_time = context.dagster_run.start_time
    end_time = context.dagster_run.end_time
    
    if start_time and end_time:
        execution_time = (end_time - start_time).total_seconds()
        
        # Get compute kind from tags
        compute_kind = context.dagster_run.tags.get("compute_kind", "unknown")
        
        # Log cost tracking information
        context.log.info(
            f"Cost tracking: {context.step_key} executed in {execution_time:.2f}s on {compute_kind}",
            extra={
                "run_id": context.run_id,
                "step_key": context.step_key,
                "execution_time_seconds": execution_time,
                "compute_kind": compute_kind,
                "asset_key": str(context.asset_key) if context.asset_key else None,
            },
        )


@success_hook
def data_lineage_hook(context: HookContext) -> None:
    """Track data lineage and dependencies."""
    
    if context.asset_key:
        # Get asset dependencies
        asset_deps = context.asset_lineage_info
        
        # Log lineage information
        context.log.info(
            f"Data lineage: {context.asset_key} materialized",
            extra={
                "run_id": context.run_id,
                "asset_key": str(context.asset_key),
                "upstream_assets": [str(dep) for dep in asset_deps.upstream_assets] if asset_deps else [],
                "downstream_assets": [str(dep) for dep in asset_deps.downstream_assets] if asset_deps else [],
                "materialization_time": context.dagster_run.end_time,
            },
        )


@failure_hook
def retry_hook(context: HookContext) -> None:
    """Handle retry logic for failed executions."""
    
    # Get retry configuration from tags
    max_retries = int(context.dagster_run.tags.get("max_retries", "0"))
    current_retry = int(context.dagster_run.tags.get("retry_count", "0"))
    
    if current_retry < max_retries:
        context.log.info(
            f"Execution failed, retry {current_retry + 1} of {max_retries} will be attempted",
            extra={
                "run_id": context.run_id,
                "step_key": context.step_key,
                "current_retry": current_retry,
                "max_retries": max_retries,
                "failure_reason": str(context.failure_data),
            },
        )
    else:
        context.log.error(
            f"Maximum retries ({max_retries}) reached for {context.step_key}",
            extra={
                "run_id": context.run_id,
                "step_key": context.step_key,
                "max_retries": max_retries,
                "failure_reason": str(context.failure_data),
            },
        )


def create_custom_success_hook(
    hook_name: str,
    custom_logic: callable,
    tags: Optional[Dict[str, str]] = None,
):
    """
    Create a custom success hook with user-defined logic.
    
    Args:
        hook_name: Name of the hook
        custom_logic: Function to execute on success
        tags: Optional tags to apply to the hook
    
    Returns:
        Configured success hook
    """
    
    @success_hook(name=hook_name, tags=tags)
    def custom_success_hook(context: HookContext) -> None:
        """Custom success hook with user-defined logic."""
        try:
            custom_logic(context)
        except Exception as e:
            context.log.error(f"Custom success hook {hook_name} failed: {e}")
    
    return custom_success_hook


def create_custom_failure_hook(
    hook_name: str,
    custom_logic: callable,
    tags: Optional[Dict[str, str]] = None,
):
    """
    Create a custom failure hook with user-defined logic.
    
    Args:
        hook_name: Name of the hook
        custom_logic: Function to execute on failure
        tags: Optional tags to apply to the hook
    
    Returns:
        Configured failure hook
    """
    
    @failure_hook(name=hook_name, tags=tags)
    def custom_failure_hook(context: HookContext) -> None:
        """Custom failure hook with user-defined logic."""
        try:
            custom_logic(context)
        except Exception as e:
            context.log.error(f"Custom failure hook {hook_name} failed: {e}")
    
    return custom_failure_hook