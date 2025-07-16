"""
Database resources for the Dagster platform.
"""

from typing import Any, Dict, Optional

import pandas as pd
from dagster import ConfigurableResource, resource
from pydantic import Field
from sqlalchemy import create_engine, Engine, text


class PostgresResource(ConfigurableResource):
    """Resource for interacting with PostgreSQL databases."""
    
    host: str = Field(description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(description="Database name")
    username: str = Field(description="Database username")
    password: str = Field(description="Database password")
    schema: Optional[str] = Field(default=None, description="Default schema")
    
    def get_engine(self) -> Engine:
        """Get SQLAlchemy engine for database connection."""
        connection_string = (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def execute_command(self, command: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a SQL command (INSERT, UPDATE, DELETE, etc.)."""
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text(command), params or {})
            conn.commit()
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
        """Upload a DataFrame to a database table."""
        engine = self.get_engine()
        df.to_sql(
            table_name,
            engine,
            schema=self.schema,
            if_exists=if_exists,
            index=False,
            method="multi",
        )


class RedshiftResource(ConfigurableResource):
    """Resource for interacting with Amazon Redshift."""
    
    host: str = Field(description="Redshift cluster endpoint")
    port: int = Field(default=5439, description="Redshift port")
    database: str = Field(description="Database name")
    username: str = Field(description="Database username")
    password: str = Field(description="Database password")
    schema: Optional[str] = Field(default="public", description="Default schema")
    
    def get_engine(self) -> Engine:
        """Get SQLAlchemy engine for Redshift connection."""
        connection_string = (
            f"redshift+psycopg2://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def execute_command(self, command: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a SQL command (INSERT, UPDATE, DELETE, etc.)."""
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text(command), params or {})
            conn.commit()
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
        """Upload a DataFrame to a Redshift table."""
        engine = self.get_engine()
        df.to_sql(
            table_name,
            engine,
            schema=self.schema,
            if_exists=if_exists,
            index=False,
            method="multi",
        )


class SnowflakeResource(ConfigurableResource):
    """Resource for interacting with Snowflake."""
    
    account: str = Field(description="Snowflake account identifier")
    username: str = Field(description="Snowflake username")
    password: str = Field(description="Snowflake password")
    database: str = Field(description="Database name")
    schema: Optional[str] = Field(default="PUBLIC", description="Default schema")
    warehouse: Optional[str] = Field(default=None, description="Warehouse name")
    role: Optional[str] = Field(default=None, description="Role name")
    
    def get_engine(self) -> Engine:
        """Get SQLAlchemy engine for Snowflake connection."""
        connection_params = {
            "account": self.account,
            "user": self.username,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
        }
        
        if self.warehouse:
            connection_params["warehouse"] = self.warehouse
        if self.role:
            connection_params["role"] = self.role
        
        connection_string = "snowflake://" + "&".join([f"{k}={v}" for k, v in connection_params.items()])
        return create_engine(connection_string)
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def execute_command(self, command: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a SQL command (INSERT, UPDATE, DELETE, etc.)."""
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text(command), params or {})
            conn.commit()


# Resource factory functions for backward compatibility
@resource(description="PostgreSQL database resource")
def postgres_resource(context) -> PostgresResource:
    """Legacy resource factory for PostgresResource."""
    return PostgresResource(
        host=context.resource_config["host"],
        port=context.resource_config.get("port", 5432),
        database=context.resource_config["database"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
        schema=context.resource_config.get("schema"),
    )


@resource(description="Redshift database resource")
def redshift_resource(context) -> RedshiftResource:
    """Legacy resource factory for RedshiftResource."""
    return RedshiftResource(
        host=context.resource_config["host"],
        port=context.resource_config.get("port", 5439),
        database=context.resource_config["database"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
        schema=context.resource_config.get("schema", "public"),
    )