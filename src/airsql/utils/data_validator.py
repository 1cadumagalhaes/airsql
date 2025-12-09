"""Data validation utilities for airsql operators."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ValidationResult:
    """Result of a validation check."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)


class DataValidator:
    """Validates DataFrames before loading into databases."""

    @staticmethod
    def validate_row_count(
        df: pd.DataFrame, min_rows: int = 1, warn_empty: bool = True
    ) -> ValidationResult:
        """Validate that DataFrame has minimum row count.

        Args:
            df: DataFrame to validate
            min_rows: Minimum number of rows required
            warn_empty: If True, warn when 0 rows; if False, only error

        Returns:
            ValidationResult with status and any issues
        """
        result = ValidationResult(is_valid=True)
        row_count = len(df)

        if row_count < min_rows:
            if row_count == 0 and warn_empty:
                result.add_warning('DataFrame is empty (0 rows)')
            else:
                result.add_error(
                    f'DataFrame has {row_count} rows but minimum required is {min_rows}'
                )

        return result

    @staticmethod
    def validate_columns(
        df: pd.DataFrame, expected_columns: List[str]
    ) -> ValidationResult:
        """Validate that DataFrame contains all expected columns.

        Args:
            df: DataFrame to validate
            expected_columns: List of column names that must exist

        Returns:
            ValidationResult with status and any missing columns
        """
        result = ValidationResult(is_valid=True)
        missing = [col for col in expected_columns if col not in df.columns]

        if missing:
            result.add_error(f'Missing columns: {", ".join(missing)}')

        return result

    @staticmethod
    def validate_no_nulls(
        df: pd.DataFrame, column_names: List[str], error_on_nulls: bool = False
    ) -> ValidationResult:
        """Validate that specified columns have no null values.

        Args:
            df: DataFrame to validate
            column_names: Columns to check for nulls
            error_on_nulls: If True, nulls are errors; if False, warnings

        Returns:
            ValidationResult with null counts per column
        """
        result = ValidationResult(is_valid=True)

        for col in column_names:
            if col not in df.columns:
                result.add_warning(f'Column "{col}" not found in DataFrame')
                continue

            null_count = df[col].isna().sum()
            if null_count > 0:
                msg = f'Column "{col}" has {null_count} null values'
                if error_on_nulls:
                    result.add_error(msg)
                else:
                    result.add_warning(msg)

        return result

    @staticmethod
    def validate_unique(
        df: pd.DataFrame, column_names: List[str], error_on_duplicates: bool = True
    ) -> ValidationResult:
        """Validate that specified columns contain unique values.

        Args:
            df: DataFrame to validate
            column_names: Columns to check for uniqueness
            error_on_duplicates: If True, duplicates are errors; if False, warnings

        Returns:
            ValidationResult with duplicate counts
        """
        result = ValidationResult(is_valid=True)

        for col in column_names:
            if col not in df.columns:
                result.add_warning(f'Column "{col}" not found in DataFrame')
                continue

            duplicate_count = df[col].duplicated().sum()
            if duplicate_count > 0:
                msg = f'Column "{col}" has {duplicate_count} duplicate values'
                if error_on_duplicates:
                    result.add_error(msg)
                else:
                    result.add_warning(msg)

        return result

    @staticmethod
    def validate_schema_match(
        df: pd.DataFrame, target_schema: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate that DataFrame schema matches target table schema.

        Args:
            df: DataFrame to validate
            target_schema: List of schema dicts with 'name' and 'type' keys

        Returns:
            ValidationResult with schema mismatch details
        """
        result = ValidationResult(is_valid=True)

        target_cols = {s['name'] for s in target_schema}
        df_cols = set(df.columns)

        # Check for missing columns in DataFrame
        missing = target_cols - df_cols
        if missing:
            result.add_error(f'DataFrame missing columns: {", ".join(sorted(missing))}')

        # Check for extra columns in DataFrame
        extra = df_cols - target_cols
        if extra:
            result.add_warning(
                f'DataFrame has extra columns: {", ".join(sorted(extra))}'
            )

        return result

    @staticmethod
    def validate_all(
        df: pd.DataFrame,
        min_rows: int = 1,
        expected_columns: Optional[List[str]] = None,
        validate_no_nulls_cols: Optional[List[str]] = None,
        validate_unique_cols: Optional[List[str]] = None,
        target_schema: Optional[List[Dict[str, Any]]] = None,
    ) -> ValidationResult:
        """Run multiple validations and combine results.

        Args:
            df: DataFrame to validate
            min_rows: Minimum row count required
            expected_columns: Columns that must exist
            validate_no_nulls_cols: Columns to check for nulls
            validate_unique_cols: Columns to check for uniqueness
            target_schema: Target table schema to validate against

        Returns:
            Combined ValidationResult with all issues
        """
        combined = ValidationResult(is_valid=True)

        # Row count validation
        row_result = DataValidator.validate_row_count(df, min_rows)
        combined.errors.extend(row_result.errors)
        combined.warnings.extend(row_result.warnings)
        combined.is_valid = combined.is_valid and row_result.is_valid

        # Column validation
        if expected_columns:
            col_result = DataValidator.validate_columns(df, expected_columns)
            combined.errors.extend(col_result.errors)
            combined.warnings.extend(col_result.warnings)
            combined.is_valid = combined.is_valid and col_result.is_valid

        # Null validation
        if validate_no_nulls_cols:
            null_result = DataValidator.validate_no_nulls(
                df, validate_no_nulls_cols, error_on_nulls=False
            )
            combined.errors.extend(null_result.errors)
            combined.warnings.extend(null_result.warnings)
            combined.is_valid = combined.is_valid and null_result.is_valid

        # Uniqueness validation
        if validate_unique_cols:
            unique_result = DataValidator.validate_unique(
                df, validate_unique_cols, error_on_duplicates=False
            )
            combined.errors.extend(unique_result.errors)
            combined.warnings.extend(unique_result.warnings)
            combined.is_valid = combined.is_valid and unique_result.is_valid

        # Schema validation
        if target_schema:
            schema_result = DataValidator.validate_schema_match(df, target_schema)
            combined.errors.extend(schema_result.errors)
            combined.warnings.extend(schema_result.warnings)
            combined.is_valid = combined.is_valid and schema_result.is_valid

        return combined
