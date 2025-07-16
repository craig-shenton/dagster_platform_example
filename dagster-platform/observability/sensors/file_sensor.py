"""
File-based sensors for the Dagster platform.
"""

from typing import Generator, List, Optional

from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import build_sensor_context

from platform_core.resources.aws_resources import S3Resource


def create_s3_file_sensor(
    job_name: str,
    bucket_name: str,
    key_prefix: str = "",
    file_extensions: Optional[List[str]] = None,
    sensor_name: Optional[str] = None,
) -> sensor:
    """
    Create a sensor that monitors S3 bucket for new files.
    
    Args:
        job_name: Name of the job to trigger
        bucket_name: S3 bucket to monitor
        key_prefix: Key prefix to filter files
        file_extensions: List of file extensions to monitor (e.g., ['.csv', '.json'])
        sensor_name: Custom sensor name
    
    Returns:
        Configured sensor
    """
    
    @sensor(
        name=sensor_name or f"s3_file_sensor_{job_name}",
        job_name=job_name,
        description=f"Monitors S3 bucket {bucket_name} for new files",
    )
    def s3_file_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor that monitors S3 bucket for new files."""
        
        # Get S3 resource from context
        s3_resource = S3Resource(bucket_name=bucket_name)
        
        # Get cursor (last processed file timestamp)
        cursor = context.cursor
        last_processed_time = cursor if cursor else "1970-01-01T00:00:00Z"
        
        # List objects in bucket
        try:
            objects = s3_resource.list_objects(prefix=key_prefix)
        except Exception as e:
            context.log.error(f"Failed to list S3 objects: {e}")
            return SensorResult(skip_reason=f"S3 error: {e}")
        
        # Filter objects by extension if specified
        if file_extensions:
            objects = [
                obj for obj in objects
                if any(obj.endswith(ext) for ext in file_extensions)
            ]
        
        # For this example, we'll trigger on any new objects
        # In a real implementation, you'd track modification times
        if objects:
            run_requests = []
            for obj in objects:
                run_requests.append(
                    RunRequest(
                        run_key=f"s3_file_{obj}",
                        run_config={
                            "resources": {
                                "s3": {
                                    "config": {
                                        "bucket_name": bucket_name,
                                    }
                                }
                            },
                            "ops": {
                                "process_file": {
                                    "config": {
                                        "file_key": obj,
                                        "bucket_name": bucket_name,
                                    }
                                }
                            }
                        }
                    )
                )
            
            return SensorResult(
                run_requests=run_requests,
                cursor=last_processed_time,  # Update cursor
            )
        
        return SensorResult(skip_reason="No new files found")
    
    return s3_file_sensor


def create_local_file_sensor(
    job_name: str,
    directory_path: str,
    file_extensions: Optional[List[str]] = None,
    sensor_name: Optional[str] = None,
) -> sensor:
    """
    Create a sensor that monitors local directory for new files.
    
    Args:
        job_name: Name of the job to trigger
        directory_path: Local directory to monitor
        file_extensions: List of file extensions to monitor
        sensor_name: Custom sensor name
    
    Returns:
        Configured sensor
    """
    import os
    from pathlib import Path
    
    @sensor(
        name=sensor_name or f"local_file_sensor_{job_name}",
        job_name=job_name,
        description=f"Monitors local directory {directory_path} for new files",
    )
    def local_file_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor that monitors local directory for new files."""
        
        # Get cursor (last processed timestamp)
        cursor = context.cursor
        last_processed_time = float(cursor) if cursor else 0
        
        # Check if directory exists
        if not os.path.exists(directory_path):
            return SensorResult(skip_reason=f"Directory {directory_path} does not exist")
        
        # Find new files
        new_files = []
        current_time = 0
        
        for file_path in Path(directory_path).rglob("*"):
            if file_path.is_file():
                # Check file extension if specified
                if file_extensions and not any(str(file_path).endswith(ext) for ext in file_extensions):
                    continue
                
                # Check modification time
                mod_time = file_path.stat().st_mtime
                current_time = max(current_time, mod_time)
                
                if mod_time > last_processed_time:
                    new_files.append(str(file_path))
        
        if new_files:
            run_requests = []
            for file_path in new_files:
                run_requests.append(
                    RunRequest(
                        run_key=f"local_file_{Path(file_path).name}_{int(current_time)}",
                        run_config={
                            "ops": {
                                "process_file": {
                                    "config": {
                                        "file_path": file_path,
                                    }
                                }
                            }
                        }
                    )
                )
            
            return SensorResult(
                run_requests=run_requests,
                cursor=str(current_time),
            )
        
        return SensorResult(skip_reason="No new files found")
    
    return local_file_sensor