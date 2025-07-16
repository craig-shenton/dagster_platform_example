"""
AWS-specific resources for the Dagster platform.
"""

from typing import Any, Dict, Optional

import boto3
from dagster import ConfigurableResource, resource
from pydantic import Field


class S3Resource(ConfigurableResource):
    """Resource for interacting with AWS S3."""
    
    bucket_name: str = Field(description="S3 bucket name")
    region_name: str = Field(default="eu-west-2", description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    
    def get_client(self) -> boto3.client:
        """Get S3 client with configured credentials."""
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return session.client("s3")
    
    def upload_file(self, file_path: str, s3_key: str) -> None:
        """Upload a file to S3."""
        client = self.get_client()
        client.upload_file(file_path, self.bucket_name, s3_key)
    
    def download_file(self, s3_key: str, file_path: str) -> None:
        """Download a file from S3."""
        client = self.get_client()
        client.download_file(self.bucket_name, s3_key, file_path)
    
    def list_objects(self, prefix: str = "") -> list[str]:
        """List objects in S3 bucket with optional prefix."""
        client = self.get_client()
        response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]


class SecretsManagerResource(ConfigurableResource):
    """Resource for interacting with AWS Secrets Manager."""
    
    region_name: str = Field(default="eu-west-2", description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    
    def get_client(self) -> boto3.client:
        """Get Secrets Manager client with configured credentials."""
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return session.client("secretsmanager")
    
    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Retrieve a secret from AWS Secrets Manager."""
        client = self.get_client()
        response = client.get_secret_value(SecretId=secret_name)
        return response["SecretString"]


class LambdaResource(ConfigurableResource):
    """Resource for invoking AWS Lambda functions."""
    
    region_name: str = Field(default="eu-west-2", description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    
    def get_client(self) -> boto3.client:
        """Get Lambda client with configured credentials."""
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return session.client("lambda")
    
    def invoke_function(self, function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke a Lambda function with the given payload."""
        import json
        
        client = self.get_client()
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        
        return json.loads(response["Payload"].read())


# Resource factory functions for backward compatibility
@resource(description="S3 resource for file operations")
def s3_resource(context) -> S3Resource:
    """Legacy resource factory for S3Resource."""
    return S3Resource(
        bucket_name=context.resource_config["bucket_name"],
        region_name=context.resource_config.get("region_name", "eu-west-2"),
        aws_access_key_id=context.resource_config.get("aws_access_key_id"),
        aws_secret_access_key=context.resource_config.get("aws_secret_access_key"),
    )


@resource(description="Secrets Manager resource for credential management")
def secrets_manager_resource(context) -> SecretsManagerResource:
    """Legacy resource factory for SecretsManagerResource."""
    return SecretsManagerResource(
        region_name=context.resource_config.get("region_name", "eu-west-2"),
        aws_access_key_id=context.resource_config.get("aws_access_key_id"),
        aws_secret_access_key=context.resource_config.get("aws_secret_access_key"),
    )