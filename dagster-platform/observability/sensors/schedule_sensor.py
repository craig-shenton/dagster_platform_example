"""
Schedule-based sensors for the Dagster platform.
"""

from typing import Optional

from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from datetime import datetime, timedelta


def create_failure_recovery_sensor(
    job_name: str,
    max_retries: int = 3,
    retry_delay_minutes: int = 5,
    sensor_name: Optional[str] = None,
) -> sensor:
    """
    Create a sensor that monitors job failures and triggers retries.
    
    Args:
        job_name: Name of the job to monitor
        max_retries: Maximum number of retries
        retry_delay_minutes: Delay between retries in minutes
        sensor_name: Custom sensor name
    
    Returns:
        Configured sensor
    """
    
    @sensor(
        name=sensor_name or f"failure_recovery_sensor_{job_name}",
        job_name=job_name,
        description=f"Monitors {job_name} for failures and triggers retries",
    )
    def failure_recovery_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor that monitors job failures and triggers retries."""
        
        # Get recent runs for the job
        runs = context.instance.get_runs(
            filters=context.instance.get_run_records(
                limit=10,
                order_by="created_at",
                ascending=False,
            )
        )
        
        # Filter runs for the specific job
        job_runs = [run for run in runs if run.job_name == job_name]
        
        if not job_runs:
            return SensorResult(skip_reason="No recent runs found")
        
        # Check the most recent run
        latest_run = job_runs[0]
        
        # Only process failed runs
        if not latest_run.is_failure:
            return SensorResult(skip_reason="Latest run did not fail")
        
        # Check if we should retry (based on tags and delay)
        retry_count = int(latest_run.tags.get("retry_count", "0"))
        
        if retry_count >= max_retries:
            return SensorResult(skip_reason=f"Maximum retries ({max_retries}) reached")
        
        # Check if enough time has passed since the failure
        failure_time = latest_run.end_time
        if failure_time:
            time_since_failure = datetime.now() - failure_time
            if time_since_failure < timedelta(minutes=retry_delay_minutes):
                return SensorResult(skip_reason=f"Waiting {retry_delay_minutes} minutes before retry")
        
        # Create retry run request
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=f"retry_{latest_run.run_id}_{retry_count + 1}",
                    tags={
                        "retry_count": str(retry_count + 1),
                        "original_run_id": latest_run.run_id,
                        "retry_reason": "automatic_failure_recovery",
                    },
                    run_config=latest_run.run_config,
                )
            ]
        )
    
    return failure_recovery_sensor


def create_sla_monitoring_sensor(
    job_name: str,
    sla_minutes: int = 60,
    sensor_name: Optional[str] = None,
) -> sensor:
    """
    Create a sensor that monitors job SLA violations.
    
    Args:
        job_name: Name of the job to monitor
        sla_minutes: SLA threshold in minutes
        sensor_name: Custom sensor name
    
    Returns:
        Configured sensor
    """
    
    @sensor(
        name=sensor_name or f"sla_monitoring_sensor_{job_name}",
        description=f"Monitors {job_name} for SLA violations (>{sla_minutes} minutes)",
    )
    def sla_monitoring_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor that monitors job SLA violations."""
        
        # Get recent runs for the job
        runs = context.instance.get_runs(
            filters=context.instance.get_run_records(
                limit=10,
                order_by="created_at",
                ascending=False,
            )
        )
        
        # Filter runs for the specific job
        job_runs = [run for run in runs if run.job_name == job_name]
        
        if not job_runs:
            return SensorResult(skip_reason="No recent runs found")
        
        # Check running jobs for SLA violations
        running_runs = [run for run in job_runs if run.is_running]
        
        sla_violations = []
        for run in running_runs:
            if run.start_time:
                runtime = datetime.now() - run.start_time
                if runtime > timedelta(minutes=sla_minutes):
                    sla_violations.append({
                        "run_id": run.run_id,
                        "runtime_minutes": runtime.total_seconds() / 60,
                        "sla_minutes": sla_minutes,
                    })
        
        if sla_violations:
            # Log SLA violations (in a real implementation, you might send alerts)
            for violation in sla_violations:
                context.log.warning(
                    f"SLA violation detected: Run {violation['run_id']} "
                    f"has been running for {violation['runtime_minutes']:.1f} minutes "
                    f"(SLA: {violation['sla_minutes']} minutes)"
                )
            
            return SensorResult(
                skip_reason=f"SLA violations detected for {len(sla_violations)} runs"
            )
        
        return SensorResult(skip_reason="No SLA violations detected")
    
    return sla_monitoring_sensor


def create_dependency_sensor(
    job_name: str,
    upstream_job_name: str,
    sensor_name: Optional[str] = None,
) -> sensor:
    """
    Create a sensor that triggers a job when an upstream job succeeds.
    
    Args:
        job_name: Name of the job to trigger
        upstream_job_name: Name of the upstream job to monitor
        sensor_name: Custom sensor name
    
    Returns:
        Configured sensor
    """
    
    @sensor(
        name=sensor_name or f"dependency_sensor_{job_name}",
        job_name=job_name,
        description=f"Triggers {job_name} when {upstream_job_name} succeeds",
    )
    def dependency_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor that triggers job based on upstream job success."""
        
        # Get cursor (last processed run ID)
        cursor = context.cursor
        last_processed_run_id = cursor if cursor else ""
        
        # Get recent runs for the upstream job
        runs = context.instance.get_runs(
            filters=context.instance.get_run_records(
                limit=10,
                order_by="created_at",
                ascending=False,
            )
        )
        
        # Filter runs for the upstream job
        upstream_runs = [run for run in runs if run.job_name == upstream_job_name]
        
        if not upstream_runs:
            return SensorResult(skip_reason="No recent upstream runs found")
        
        # Find successful runs that haven't been processed
        new_successful_runs = []
        for run in upstream_runs:
            if run.is_success and run.run_id != last_processed_run_id:
                new_successful_runs.append(run)
        
        if new_successful_runs:
            # Take the most recent successful run
            latest_successful_run = new_successful_runs[0]
            
            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"triggered_by_{latest_successful_run.run_id}",
                        tags={
                            "triggered_by": latest_successful_run.run_id,
                            "upstream_job": upstream_job_name,
                        },
                    )
                ],
                cursor=latest_successful_run.run_id,
            )
        
        return SensorResult(skip_reason="No new successful upstream runs")
    
    return dependency_sensor