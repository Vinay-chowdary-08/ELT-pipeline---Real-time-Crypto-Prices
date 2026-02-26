"""
Data quality tests: schema validation and null checks on API response and processed data.

Ensures the pipeline produces analytics-ready data with expected structure
and no nulls in critical columns.
"""

from pathlib import Path

import polars as pl
import pytest

from src.transformation.main import read_raw_latest, transform

# Required columns that must exist in the API response and transformed output
REQUIRED_SCHEMA = frozenset(
    [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "market_cap_rank",
    ]
)

# Columns that must not contain nulls for downstream analytics
CRITICAL_COLUMNS_NO_NULL = ["id", "symbol", "name", "current_price", "market_cap_rank"]


def validate_api_response_schema(records: list[dict]) -> None:
    """
    Assert API response has required keys and no nulls in critical columns.

    Raises:
        AssertionError: If schema or null constraints are violated.
    """
    assert len(records) > 0, "API response must contain at least one record"
    for i, row in enumerate(records):
        missing = REQUIRED_SCHEMA - set(row.keys())
        assert not missing, (
            f"Record {i} missing required keys: {missing}. Keys present: {list(row.keys())}"
        )
        for col in CRITICAL_COLUMNS_NO_NULL:
            assert col in row, f"Record {i} missing critical column: {col}"
            assert row[col] is not None, f"Record {i} has null in critical column '{col}'"
            if col in ("id", "symbol", "name"):
                assert isinstance(row[col], str), (
                    f"Record {i} column '{col}' must be string, got {type(row[col])}"
                )
            else:
                assert isinstance(row[col], (int, float)), (
                    f"Record {i} column '{col}' must be numeric, got {type(row[col])}"
                )


def validate_dataframe_schema_and_nulls(df: pl.DataFrame) -> None:
    """
    Assert DataFrame has required columns and no nulls in critical columns.

    Raises:
        AssertionError: If schema or null constraints are violated.
    """
    assert not df.is_empty(), "DataFrame must not be empty"
    missing = REQUIRED_SCHEMA - set(df.columns)
    assert not missing, (
        f"DataFrame missing required columns: {missing}. Columns present: {df.columns}"
    )
    for col in CRITICAL_COLUMNS_NO_NULL:
        assert col in df.columns, f"DataFrame missing critical column: {col}"
        null_count = df[col].null_count()
        assert null_count == 0, f"Critical column '{col}' has {null_count} null(s); expected 0"


class TestAPIResponseSchema:
    """Data quality tests against API response structure."""

    def test_sample_payload_has_required_schema(self, sample_crypto_payload: list[dict]) -> None:
        """API response must contain required keys on every record."""
        validate_api_response_schema(sample_crypto_payload)

    def test_sample_payload_no_nulls_in_critical_columns(
        self, sample_crypto_payload: list[dict]
    ) -> None:
        """Critical columns (id, symbol, name, current_price, market_cap_rank) must not be null."""
        for row in sample_crypto_payload:
            for col in CRITICAL_COLUMNS_NO_NULL:
                assert row.get(col) is not None, f"Null in critical column '{col}': {row}"

    def test_empty_response_fails_validation(self) -> None:
        """Empty API response must fail schema validation."""
        with pytest.raises(AssertionError, match="at least one record"):
            validate_api_response_schema([])

    def test_missing_required_key_fails_validation(self) -> None:
        """Record missing required key must fail validation."""
        bad_record = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                # missing "name", "current_price", "market_cap", "market_cap_rank"
            }
        ]
        with pytest.raises(AssertionError, match="missing required"):
            validate_api_response_schema(bad_record)

    def test_null_in_critical_column_fails_validation(self) -> None:
        """Null in critical column must fail validation."""
        bad_record = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": None,  # invalid
                "market_cap": 1e12,
                "market_cap_rank": 1,
            }
        ]
        with pytest.raises(AssertionError, match="null"):
            validate_api_response_schema(bad_record)


class TestTransformedDataSchema:
    """Data quality tests against transformed DataFrame (Polars)."""

    def test_transformed_df_has_required_schema(self, sample_crypto_payload: list[dict]) -> None:
        """Transformed DataFrame must have required columns."""
        df = pl.DataFrame(sample_crypto_payload)
        df = transform(df)
        validate_dataframe_schema_and_nulls(df)

    def test_transformed_df_no_nulls_in_critical_columns(
        self, sample_crypto_payload: list[dict]
    ) -> None:
        """Transformed DataFrame must have no nulls in critical columns."""
        df = pl.DataFrame(sample_crypto_payload)
        df = transform(df)
        for col in CRITICAL_COLUMNS_NO_NULL:
            assert col in df.columns
            assert df[col].null_count() == 0, f"Unexpected nulls in {col}"

    def test_read_raw_latest_output_schema(self, raw_json_file: Path) -> None:
        """Data read from raw JSON and transformed must satisfy schema and null checks."""
        raw_dir = raw_json_file.parent
        df = read_raw_latest(raw_dir=raw_dir)
        assert not df.is_empty()
        df = transform(df)
        validate_dataframe_schema_and_nulls(df)

    def test_empty_dataframe_fails_validation(self) -> None:
        """Empty DataFrame must fail schema validation."""
        df = pl.DataFrame()
        with pytest.raises(AssertionError, match="must not be empty"):
            validate_dataframe_schema_and_nulls(df)
