"""
Asset factory functions for common patterns.
"""

from typing import Any, Callable, Dict, List, Optional, Union

from dagster import AssetIn, AssetOut, Config, asset, multi_asset
from dagster._core.definitions.asset_spec import AssetSpec
import pandas as pd

from platform_core.resources.database_resources import PostgresResource
from platform_core.resources.aws_resources import S3Resource


def create_ingestion_asset(
    name: str,
    source_config: Dict[str, Any],
    output_path: Optional[str] = None,
    description: Optional[str] = None,
):
    """
    Factory function to create data ingestion assets.
    
    Args:
        name: Asset name
        source_config: Source configuration (database, API, file, etc.)
        output_path: Output path for the asset
        description: Asset description
    
    Returns:
        Configured asset
    """
    
    @asset(
        name=name,
        description=description or f"Ingestion asset for {name}",
        metadata={
            "tier": "bronze",
            "source_config": source_config,
            "output_path": output_path,
        },
        tags={
            "tier": "bronze",
            "pattern": "ingestion",
        },
    )
    def ingestion_asset(context) -> pd.DataFrame:
        """Generic ingestion asset."""
        
        source_type = source_config.get("type")
        
        if source_type == "database":
            # Database ingestion
            db_resource = PostgresResource(**source_config.get("connection", {}))
            query = source_config.get("query")
            
            if not query:
                raise ValueError("Database source requires 'query' in config")
            
            context.log.info(f"Executing database query for {name}")
            df = db_resource.execute_query(query)
            
        elif source_type == "s3":
            # S3 file ingestion
            s3_resource = S3Resource(**source_config.get("connection", {}))
            file_key = source_config.get("file_key")
            file_format = source_config.get("format", "csv")
            
            if not file_key:
                raise ValueError("S3 source requires 'file_key' in config")
            
            context.log.info(f"Downloading file from S3: {file_key}")
            local_path = f"/tmp/{file_key.split('/')[-1]}"
            s3_resource.download_file(file_key, local_path)
            
            # Read file based on format
            if file_format == "csv":
                df = pd.read_csv(local_path)
            elif file_format == "json":
                df = pd.read_json(local_path)
            elif file_format == "parquet":
                df = pd.read_parquet(local_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        
        elif source_type == "api":
            # API ingestion
            from platform_core.resources.api_resources import HTTPResource
            
            api_resource = HTTPResource(**source_config.get("connection", {}))
            endpoint = source_config.get("endpoint")
            
            if not endpoint:
                raise ValueError("API source requires 'endpoint' in config")
            
            context.log.info(f"Calling API endpoint: {endpoint}")
            response = api_resource.get(endpoint)
            
            # Convert API response to DataFrame
            data = response.json()
            df = pd.DataFrame(data if isinstance(data, list) else [data])
        
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        context.log.info(f"Ingested {len(df)} rows for {name}")
        
        # Save to output path if specified
        if output_path:
            df.to_parquet(output_path, index=False)
            context.log.info(f"Saved data to {output_path}")
        
        return df
    
    return ingestion_asset


def create_transformation_asset(
    name: str,
    upstream_assets: List[str],
    transformation_config: Dict[str, Any],
    description: Optional[str] = None,
):
    """
    Factory function to create data transformation assets.
    
    Args:
        name: Asset name
        upstream_assets: List of upstream asset names
        transformation_config: Transformation configuration
        description: Asset description
    
    Returns:
        Configured asset
    """
    
    # Create asset ins for upstream dependencies
    asset_ins = {
        asset_name: AssetIn(asset_name)
        for asset_name in upstream_assets
    }
    
    @asset(
        name=name,
        ins=asset_ins,
        description=description or f"Transformation asset for {name}",
        metadata={
            "tier": "silver",
            "transformation_config": transformation_config,
        },
        tags={
            "tier": "silver",
            "pattern": "transformation",
        },
    )
    def transformation_asset(context, **upstream_data) -> pd.DataFrame:
        """Generic transformation asset."""
        
        transformation_type = transformation_config.get("type")
        
        # Get the primary upstream DataFrame
        primary_upstream = transformation_config.get("primary_upstream", upstream_assets[0])
        df = upstream_data[primary_upstream]
        
        context.log.info(f"Starting transformation for {name} with {len(df)} rows")
        
        if transformation_type == "filter":
            # Filter transformation
            filter_expr = transformation_config.get("filter_expression")
            if filter_expr:
                df = df.query(filter_expr)
                context.log.info(f"Filtered to {len(df)} rows")
        
        elif transformation_type == "aggregate":
            # Aggregation transformation
            group_by = transformation_config.get("group_by", [])
            aggregations = transformation_config.get("aggregations", {})
            
            if group_by and aggregations:
                df = df.groupby(group_by).agg(aggregations).reset_index()
                context.log.info(f"Aggregated to {len(df)} groups")
        
        elif transformation_type == "join":
            # Join transformation
            join_asset = transformation_config.get("join_asset")
            join_type = transformation_config.get("join_type", "inner")
            join_keys = transformation_config.get("join_keys", [])
            
            if join_asset and join_asset in upstream_data:
                right_df = upstream_data[join_asset]
                df = df.merge(right_df, on=join_keys, how=join_type)
                context.log.info(f"Joined with {join_asset}, result: {len(df)} rows")
        
        elif transformation_type == "custom":
            # Custom transformation
            custom_function = transformation_config.get("function")
            if custom_function:
                df = custom_function(df)
                context.log.info(f"Custom transformation applied, result: {len(df)} rows")
        
        else:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
        
        context.log.info(f"Transformation completed for {name}")
        return df
    
    return transformation_asset


def create_output_asset(
    name: str,
    upstream_asset: str,
    output_config: Dict[str, Any],
    description: Optional[str] = None,
):
    """
    Factory function to create output/export assets.
    
    Args:
        name: Asset name
        upstream_asset: Upstream asset name
        output_config: Output configuration
        description: Asset description
    
    Returns:
        Configured asset
    """
    
    @asset(
        name=name,
        ins={upstream_asset: AssetIn(upstream_asset)},
        description=description or f"Output asset for {name}",
        metadata={
            "tier": "gold",
            "output_config": output_config,
        },
        tags={
            "tier": "gold",
            "pattern": "output",
        },
    )
    def output_asset(context, **upstream_data) -> None:
        """Generic output asset."""
        
        df = upstream_data[upstream_asset]
        output_type = output_config.get("type")
        
        context.log.info(f"Outputting {len(df)} rows for {name}")
        
        if output_type == "database":
            # Database output
            db_resource = PostgresResource(**output_config.get("connection", {}))
            table_name = output_config.get("table_name")
            if_exists = output_config.get("if_exists", "replace")
            
            if not table_name:
                raise ValueError("Database output requires 'table_name' in config")
            
            context.log.info(f"Writing to database table: {table_name}")
            db_resource.upload_dataframe(df, table_name, if_exists)
        
        elif output_type == "s3":
            # S3 output
            s3_resource = S3Resource(**output_config.get("connection", {}))
            file_key = output_config.get("file_key")
            file_format = output_config.get("format", "parquet")
            
            if not file_key:
                raise ValueError("S3 output requires 'file_key' in config")
            
            # Write file locally first
            local_path = f"/tmp/{file_key.split('/')[-1]}"
            
            if file_format == "csv":
                df.to_csv(local_path, index=False)
            elif file_format == "json":
                df.to_json(local_path, orient="records")
            elif file_format == "parquet":
                df.to_parquet(local_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            context.log.info(f"Uploading to S3: {file_key}")
            s3_resource.upload_file(local_path, file_key)
        
        elif output_type == "api":
            # API output
            from platform_core.resources.api_resources import HTTPResource
            
            api_resource = HTTPResource(**output_config.get("connection", {}))
            endpoint = output_config.get("endpoint")
            
            if not endpoint:
                raise ValueError("API output requires 'endpoint' in config")
            
            # Convert DataFrame to JSON for API
            data = df.to_dict(orient="records")
            
            context.log.info(f"Posting to API endpoint: {endpoint}")
            response = api_resource.post(endpoint, data)
            context.log.info(f"API response status: {response.status_code}")
        
        else:
            raise ValueError(f"Unsupported output type: {output_type}")
        
        context.log.info(f"Output completed for {name}")
    
    return output_asset


def create_multi_output_asset(
    name: str,
    upstream_asset: str,
    output_specs: List[AssetSpec],
    split_logic: Callable[[pd.DataFrame], Dict[str, pd.DataFrame]],
    description: Optional[str] = None,
):
    """
    Factory function to create multi-output assets.
    
    Args:
        name: Asset name prefix
        upstream_asset: Upstream asset name
        output_specs: List of output asset specifications
        split_logic: Function to split DataFrame into multiple outputs
        description: Asset description
    
    Returns:
        Configured multi-asset
    """
    
    @multi_asset(
        name=name,
        ins={upstream_asset: AssetIn(upstream_asset)},
        specs=output_specs,
        description=description or f"Multi-output asset for {name}",
    )
    def multi_output_asset(context, **upstream_data) -> Dict[str, pd.DataFrame]:
        """Generic multi-output asset."""
        
        df = upstream_data[upstream_asset]
        
        context.log.info(f"Splitting {len(df)} rows into {len(output_specs)} outputs")
        
        # Apply split logic
        outputs = split_logic(df)
        
        # Log output sizes
        for output_name, output_df in outputs.items():
            context.log.info(f"Output {output_name}: {len(output_df)} rows")
        
        return outputs
    
    return multi_output_asset