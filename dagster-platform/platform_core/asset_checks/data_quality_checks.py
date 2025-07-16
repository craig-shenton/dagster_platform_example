"""
Data quality checks for asset validation.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue, asset_check


def check_no_nulls(df: pd.DataFrame, columns: List[str]) -> AssetCheckResult:
    """Check that specified columns have no null values."""
    null_counts = df[columns].isnull().sum()
    failed_columns = null_counts[null_counts > 0].index.tolist()
    
    if failed_columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Found null values in columns: {', '.join(failed_columns)}",
            metadata={
                "failed_columns": MetadataValue.json(failed_columns),
                "null_counts": MetadataValue.json(null_counts.to_dict()),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"No null values found in {len(columns)} columns",
        metadata={
            "checked_columns": MetadataValue.json(columns),
            "total_rows": MetadataValue.int(len(df)),
        },
    )


def check_unique_values(df: pd.DataFrame, columns: List[str]) -> AssetCheckResult:
    """Check that specified columns have unique values."""
    duplicate_counts = {}
    failed_columns = []
    
    for column in columns:
        duplicate_count = df[column].duplicated().sum()
        duplicate_counts[column] = duplicate_count
        if duplicate_count > 0:
            failed_columns.append(column)
    
    if failed_columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Found duplicate values in columns: {', '.join(failed_columns)}",
            metadata={
                "failed_columns": MetadataValue.json(failed_columns),
                "duplicate_counts": MetadataValue.json(duplicate_counts),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"All values unique in {len(columns)} columns",
        metadata={
            "checked_columns": MetadataValue.json(columns),
            "total_rows": MetadataValue.int(len(df)),
        },
    )


def check_row_count(df: pd.DataFrame, min_rows: int = 1, max_rows: Optional[int] = None) -> AssetCheckResult:
    """Check that DataFrame has expected number of rows."""
    row_count = len(df)
    
    if row_count < min_rows:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Row count {row_count} below minimum {min_rows}",
            metadata={
                "row_count": MetadataValue.int(row_count),
                "min_rows": MetadataValue.int(min_rows),
                "max_rows": MetadataValue.int(max_rows) if max_rows else None,
            },
        )
    
    if max_rows and row_count > max_rows:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Row count {row_count} above maximum {max_rows}",
            metadata={
                "row_count": MetadataValue.int(row_count),
                "min_rows": MetadataValue.int(min_rows),
                "max_rows": MetadataValue.int(max_rows),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Row count {row_count} within expected range",
        metadata={
            "row_count": MetadataValue.int(row_count),
            "min_rows": MetadataValue.int(min_rows),
            "max_rows": MetadataValue.int(max_rows) if max_rows else None,
        },
    )


def check_column_values(df: pd.DataFrame, column: str, allowed_values: List[Any]) -> AssetCheckResult:
    """Check that column values are within allowed set."""
    unique_values = df[column].unique()
    invalid_values = [val for val in unique_values if val not in allowed_values]
    
    if invalid_values:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Found invalid values in column {column}: {invalid_values}",
            metadata={
                "column": MetadataValue.text(column),
                "invalid_values": MetadataValue.json(invalid_values),
                "allowed_values": MetadataValue.json(allowed_values),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"All values in column {column} are valid",
        metadata={
            "column": MetadataValue.text(column),
            "unique_values": MetadataValue.json(unique_values.tolist()),
            "allowed_values": MetadataValue.json(allowed_values),
        },
    )


def check_numeric_range(df: pd.DataFrame, column: str, min_value: Optional[float] = None, max_value: Optional[float] = None) -> AssetCheckResult:
    """Check that numeric column values are within expected range."""
    if min_value is not None:
        below_min = (df[column] < min_value).sum()
        if below_min > 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"{below_min} values in column {column} below minimum {min_value}",
                metadata={
                    "column": MetadataValue.text(column),
                    "below_min_count": MetadataValue.int(below_min),
                    "min_value": MetadataValue.float(min_value),
                    "actual_min": MetadataValue.float(df[column].min()),
                },
            )
    
    if max_value is not None:
        above_max = (df[column] > max_value).sum()
        if above_max > 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"{above_max} values in column {column} above maximum {max_value}",
                metadata={
                    "column": MetadataValue.text(column),
                    "above_max_count": MetadataValue.int(above_max),
                    "max_value": MetadataValue.float(max_value),
                    "actual_max": MetadataValue.float(df[column].max()),
                },
            )
    
    return AssetCheckResult(
        passed=True,
        description=f"All values in column {column} within expected range",
        metadata={
            "column": MetadataValue.text(column),
            "min_value": MetadataValue.float(min_value) if min_value is not None else None,
            "max_value": MetadataValue.float(max_value) if max_value is not None else None,
            "actual_min": MetadataValue.float(df[column].min()),
            "actual_max": MetadataValue.float(df[column].max()),
        },
    )


def check_date_range(df: pd.DataFrame, column: str, min_date: Optional[str] = None, max_date: Optional[str] = None) -> AssetCheckResult:
    """Check that date column values are within expected range."""
    df[column] = pd.to_datetime(df[column])
    
    if min_date is not None:
        min_date_parsed = pd.to_datetime(min_date)
        before_min = (df[column] < min_date_parsed).sum()
        if before_min > 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"{before_min} dates in column {column} before minimum {min_date}",
                metadata={
                    "column": MetadataValue.text(column),
                    "before_min_count": MetadataValue.int(before_min),
                    "min_date": MetadataValue.text(min_date),
                    "actual_min": MetadataValue.text(str(df[column].min())),
                },
            )
    
    if max_date is not None:
        max_date_parsed = pd.to_datetime(max_date)
        after_max = (df[column] > max_date_parsed).sum()
        if after_max > 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"{after_max} dates in column {column} after maximum {max_date}",
                metadata={
                    "column": MetadataValue.text(column),
                    "after_max_count": MetadataValue.int(after_max),
                    "max_date": MetadataValue.text(max_date),
                    "actual_max": MetadataValue.text(str(df[column].max())),
                },
            )
    
    return AssetCheckResult(
        passed=True,
        description=f"All dates in column {column} within expected range",
        metadata={
            "column": MetadataValue.text(column),
            "min_date": MetadataValue.text(min_date) if min_date else None,
            "max_date": MetadataValue.text(max_date) if max_date else None,
            "actual_min": MetadataValue.text(str(df[column].min())),
            "actual_max": MetadataValue.text(str(df[column].max())),
        },
    )