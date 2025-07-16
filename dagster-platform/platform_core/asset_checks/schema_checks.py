"""
Schema validation checks for assets.
"""

from typing import Dict, List, Optional, Union

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue


def check_required_columns(df: pd.DataFrame, required_columns: List[str]) -> AssetCheckResult:
    """Check that DataFrame contains all required columns."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Missing required columns: {', '.join(missing_columns)}",
            metadata={
                "missing_columns": MetadataValue.json(missing_columns),
                "required_columns": MetadataValue.json(required_columns),
                "actual_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"All {len(required_columns)} required columns present",
        metadata={
            "required_columns": MetadataValue.json(required_columns),
            "actual_columns": MetadataValue.json(df.columns.tolist()),
        },
    )


def check_column_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> AssetCheckResult:
    """Check that DataFrame columns have expected data types."""
    type_mismatches = []
    
    for column, expected_type in expected_types.items():
        if column not in df.columns:
            continue
            
        actual_type = str(df[column].dtype)
        
        # Simple type matching - could be enhanced for more complex type checking
        if expected_type == "string" and not actual_type.startswith("object"):
            type_mismatches.append(f"{column}: expected string, got {actual_type}")
        elif expected_type == "integer" and not actual_type.startswith("int"):
            type_mismatches.append(f"{column}: expected integer, got {actual_type}")
        elif expected_type == "float" and not actual_type.startswith("float"):
            type_mismatches.append(f"{column}: expected float, got {actual_type}")
        elif expected_type == "datetime" and not actual_type.startswith("datetime"):
            type_mismatches.append(f"{column}: expected datetime, got {actual_type}")
        elif expected_type == "boolean" and not actual_type.startswith("bool"):
            type_mismatches.append(f"{column}: expected boolean, got {actual_type}")
    
    if type_mismatches:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Column type mismatches: {'; '.join(type_mismatches)}",
            metadata={
                "type_mismatches": MetadataValue.json(type_mismatches),
                "expected_types": MetadataValue.json(expected_types),
                "actual_types": MetadataValue.json({col: str(df[col].dtype) for col in df.columns}),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"All {len(expected_types)} column types match expectations",
        metadata={
            "expected_types": MetadataValue.json(expected_types),
            "actual_types": MetadataValue.json({col: str(df[col].dtype) for col in df.columns}),
        },
    )


def check_no_extra_columns(df: pd.DataFrame, allowed_columns: List[str]) -> AssetCheckResult:
    """Check that DataFrame doesn't contain unexpected columns."""
    extra_columns = [col for col in df.columns if col not in allowed_columns]
    
    if extra_columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Found unexpected columns: {', '.join(extra_columns)}",
            metadata={
                "extra_columns": MetadataValue.json(extra_columns),
                "allowed_columns": MetadataValue.json(allowed_columns),
                "actual_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description="No unexpected columns found",
        metadata={
            "allowed_columns": MetadataValue.json(allowed_columns),
            "actual_columns": MetadataValue.json(df.columns.tolist()),
        },
    )


def check_column_order(df: pd.DataFrame, expected_order: List[str]) -> AssetCheckResult:
    """Check that DataFrame columns are in expected order."""
    # Only check columns that exist in both expected and actual
    common_columns = [col for col in expected_order if col in df.columns]
    actual_order = [col for col in df.columns if col in expected_order]
    
    if common_columns != actual_order:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Column order doesn't match expected order",
            metadata={
                "expected_order": MetadataValue.json(expected_order),
                "actual_order": MetadataValue.json(actual_order),
                "common_columns": MetadataValue.json(common_columns),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Column order matches expected order for {len(common_columns)} columns",
        metadata={
            "expected_order": MetadataValue.json(expected_order),
            "actual_order": MetadataValue.json(actual_order),
        },
    )


def validate_schema(df: pd.DataFrame, schema: Dict[str, Union[str, Dict]]) -> AssetCheckResult:
    """Comprehensive schema validation combining multiple checks."""
    issues = []
    
    # Check required columns
    if "required_columns" in schema:
        result = check_required_columns(df, schema["required_columns"])
        if not result.passed:
            issues.append(result.description)
    
    # Check column types
    if "column_types" in schema:
        result = check_column_types(df, schema["column_types"])
        if not result.passed:
            issues.append(result.description)
    
    # Check for extra columns
    if "allowed_columns" in schema:
        result = check_no_extra_columns(df, schema["allowed_columns"])
        if not result.passed:
            issues.append(result.description)
    
    # Check column order
    if "column_order" in schema:
        result = check_column_order(df, schema["column_order"])
        if not result.passed:
            issues.append(result.description)
    
    if issues:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Schema validation failed: {'; '.join(issues)}",
            metadata={
                "schema_issues": MetadataValue.json(issues),
                "schema_definition": MetadataValue.json(schema),
                "actual_schema": MetadataValue.json({
                    "columns": df.columns.tolist(),
                    "types": {col: str(df[col].dtype) for col in df.columns},
                    "shape": df.shape,
                }),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description="Schema validation passed",
        metadata={
            "schema_definition": MetadataValue.json(schema),
            "actual_schema": MetadataValue.json({
                "columns": df.columns.tolist(),
                "types": {col: str(df[col].dtype) for col in df.columns},
                "shape": df.shape,
            }),
        },
    )