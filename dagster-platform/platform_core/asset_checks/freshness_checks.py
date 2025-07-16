"""
Data freshness checks for assets.
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue


def check_data_freshness(df: pd.DataFrame, timestamp_column: str, max_age_hours: int = 24) -> AssetCheckResult:
    """Check that the most recent data is within the expected freshness window."""
    if timestamp_column not in df.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Timestamp column '{timestamp_column}' not found in DataFrame",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "available_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    # Convert timestamp column to datetime
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    
    # Get the most recent timestamp
    most_recent = df[timestamp_column].max()
    current_time = datetime.now()
    
    # Calculate age in hours
    age_hours = (current_time - most_recent).total_seconds() / 3600
    
    if age_hours > max_age_hours:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Data is {age_hours:.1f} hours old, exceeds maximum age of {max_age_hours} hours",
            metadata={
                "most_recent_timestamp": MetadataValue.text(str(most_recent)),
                "current_time": MetadataValue.text(str(current_time)),
                "age_hours": MetadataValue.float(age_hours),
                "max_age_hours": MetadataValue.int(max_age_hours),
                "timestamp_column": MetadataValue.text(timestamp_column),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Data is fresh: {age_hours:.1f} hours old (within {max_age_hours} hour limit)",
        metadata={
            "most_recent_timestamp": MetadataValue.text(str(most_recent)),
            "current_time": MetadataValue.text(str(current_time)),
            "age_hours": MetadataValue.float(age_hours),
            "max_age_hours": MetadataValue.int(max_age_hours),
            "timestamp_column": MetadataValue.text(timestamp_column),
        },
    )


def check_update_frequency(df: pd.DataFrame, timestamp_column: str, expected_frequency_hours: int = 1) -> AssetCheckResult:
    """Check that data is being updated at the expected frequency."""
    if timestamp_column not in df.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Timestamp column '{timestamp_column}' not found in DataFrame",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "available_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    # Convert timestamp column to datetime and sort
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    df_sorted = df.sort_values(timestamp_column)
    
    # Calculate time differences between consecutive records
    time_diffs = df_sorted[timestamp_column].diff().dt.total_seconds() / 3600
    
    # Remove NaN (first row) and get statistics
    time_diffs = time_diffs.dropna()
    
    if len(time_diffs) == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Insufficient data to check update frequency",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "record_count": MetadataValue.int(len(df)),
            },
        )
    
    avg_frequency = time_diffs.mean()
    max_gap = time_diffs.max()
    
    # Check if there are significant gaps in updates
    threshold = expected_frequency_hours * 2  # Allow some tolerance
    
    if max_gap > threshold:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Large gap in updates detected: {max_gap:.1f} hours (expected: {expected_frequency_hours} hours)",
            metadata={
                "max_gap_hours": MetadataValue.float(max_gap),
                "avg_frequency_hours": MetadataValue.float(avg_frequency),
                "expected_frequency_hours": MetadataValue.int(expected_frequency_hours),
                "timestamp_column": MetadataValue.text(timestamp_column),
                "record_count": MetadataValue.int(len(df)),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Update frequency is normal: avg {avg_frequency:.1f} hours, max gap {max_gap:.1f} hours",
        metadata={
            "max_gap_hours": MetadataValue.float(max_gap),
            "avg_frequency_hours": MetadataValue.float(avg_frequency),
            "expected_frequency_hours": MetadataValue.int(expected_frequency_hours),
            "timestamp_column": MetadataValue.text(timestamp_column),
            "record_count": MetadataValue.int(len(df)),
        },
    )


def check_business_hours_data(df: pd.DataFrame, timestamp_column: str, business_start: int = 9, business_end: int = 17) -> AssetCheckResult:
    """Check that data is being generated during business hours."""
    if timestamp_column not in df.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Timestamp column '{timestamp_column}' not found in DataFrame",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "available_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    # Convert timestamp column to datetime
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    
    # Extract hour from timestamps
    df['hour'] = df[timestamp_column].dt.hour
    
    # Check for data during business hours
    business_hours_data = df[(df['hour'] >= business_start) & (df['hour'] < business_end)]
    business_hours_count = len(business_hours_data)
    total_count = len(df)
    business_hours_percentage = (business_hours_count / total_count) * 100 if total_count > 0 else 0
    
    # Also check for weekend data (assuming business days are Mon-Fri)
    df['weekday'] = df[timestamp_column].dt.weekday
    weekend_data = df[df['weekday'] >= 5]  # Saturday=5, Sunday=6
    weekend_count = len(weekend_data)
    weekend_percentage = (weekend_count / total_count) * 100 if total_count > 0 else 0
    
    return AssetCheckResult(
        passed=True,  # This is informational, not a failure condition
        description=f"Data distribution: {business_hours_percentage:.1f}% during business hours, {weekend_percentage:.1f}% on weekends",
        metadata={
            "business_hours_count": MetadataValue.int(business_hours_count),
            "business_hours_percentage": MetadataValue.float(business_hours_percentage),
            "weekend_count": MetadataValue.int(weekend_count),
            "weekend_percentage": MetadataValue.float(weekend_percentage),
            "total_count": MetadataValue.int(total_count),
            "business_start": MetadataValue.int(business_start),
            "business_end": MetadataValue.int(business_end),
            "timestamp_column": MetadataValue.text(timestamp_column),
        },
    )


def check_data_continuity(df: pd.DataFrame, timestamp_column: str, expected_interval_minutes: int = 60) -> AssetCheckResult:
    """Check for gaps in data continuity based on expected interval."""
    if timestamp_column not in df.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Timestamp column '{timestamp_column}' not found in DataFrame",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "available_columns": MetadataValue.json(df.columns.tolist()),
            },
        )
    
    # Convert timestamp column to datetime and sort
    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    df_sorted = df.sort_values(timestamp_column)
    
    # Calculate time differences between consecutive records
    time_diffs = df_sorted[timestamp_column].diff().dt.total_seconds() / 60  # Convert to minutes
    
    # Remove NaN (first row)
    time_diffs = time_diffs.dropna()
    
    if len(time_diffs) == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Insufficient data to check continuity",
            metadata={
                "timestamp_column": MetadataValue.text(timestamp_column),
                "record_count": MetadataValue.int(len(df)),
            },
        )
    
    # Find gaps larger than expected interval (with some tolerance)
    tolerance = expected_interval_minutes * 1.5
    gaps = time_diffs[time_diffs > tolerance]
    
    if len(gaps) > 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"Found {len(gaps)} data gaps longer than {tolerance:.1f} minutes",
            metadata={
                "gap_count": MetadataValue.int(len(gaps)),
                "largest_gap_minutes": MetadataValue.float(gaps.max()),
                "avg_gap_minutes": MetadataValue.float(gaps.mean()),
                "expected_interval_minutes": MetadataValue.int(expected_interval_minutes),
                "tolerance_minutes": MetadataValue.float(tolerance),
                "timestamp_column": MetadataValue.text(timestamp_column),
            },
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Data continuity is good: no gaps larger than {tolerance:.1f} minutes",
        metadata={
            "expected_interval_minutes": MetadataValue.int(expected_interval_minutes),
            "tolerance_minutes": MetadataValue.float(tolerance),
            "timestamp_column": MetadataValue.text(timestamp_column),
            "record_count": MetadataValue.int(len(df)),
        },
    )