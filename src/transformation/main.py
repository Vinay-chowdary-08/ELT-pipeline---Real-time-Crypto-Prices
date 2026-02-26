"""
Transformation: read raw crypto data, clean with Polars, load into DuckDB.

This is the Transform + Load step. It reads the latest raw JSON (or all),
builds a normalized schema with Polars, and upserts into a DuckDB table.
Run as:
    python -m src.transformation.main
"""

from pathlib import Path

import duckdb
import polars as pl

from src.config import DB_PATH, RAW_DATA_DIR, get_data_dir
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TransformationError(Exception):
    """Raised when transformation or load fails."""

    pass


# Schema: columns we persist (subset of CoinGecko response)
PRICE_COLUMNS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "high_24h",
    "low_24h",
    "price_change_24h",
    "price_change_percentage_24h",
    "market_cap_rank",
    "last_updated",
]


def read_raw_latest(raw_dir: Path | None = None) -> pl.DataFrame:
    """
    Read the most recent raw JSON file from the raw data directory.

    Args:
        raw_dir: Directory containing raw JSON files (default: config RAW_DATA_DIR).

    Returns:
        Polars DataFrame with selected columns; empty DataFrame if no files.
    """
    raw_dir = Path(raw_dir or RAW_DATA_DIR)
    if not raw_dir.exists():
        logger.warning("Raw directory does not exist: %s", raw_dir)
        return pl.DataFrame()

    files = sorted(raw_dir.glob("crypto_prices_*.json"), reverse=True)
    if not files:
        logger.warning("No raw JSON files found in %s", raw_dir)
        return pl.DataFrame()

    path = files[0]
    try:
        df = pl.read_json(path)
        available = [c for c in PRICE_COLUMNS if c in df.columns]
        out = df.select(available) if available else df
        logger.info("Read %d rows from %s", len(out), path.name)
        return out
    except Exception as e:
        logger.exception("Failed to read %s: %s", path, e)
        raise TransformationError(f"Failed to read raw file: {e}") from e


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and normalize the raw DataFrame for loading.

    - Cast numeric columns, parse last_updated to datetime.
    - Ensure consistent column order.

    Args:
        df: Raw DataFrame from read_raw_latest.

    Returns:
        Cleaned DataFrame ready for DuckDB.
    """
    if df.is_empty():
        return df

    out = df.clone()
    # Coerce numerics (API may return nulls)
    for col in [
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "market_cap_rank",
    ]:
        if col in out.columns:
            out = out.with_columns(pl.col(col).cast(pl.Float64))

    if "last_updated" in out.columns:
        out = out.with_columns(
            pl.col("last_updated").str.to_datetime(time_zone="UTC", strict=False)
        )
    logger.debug("Transformed %d rows", len(out))
    return out


def load_to_duckdb(df: pl.DataFrame, db_path: str = DB_PATH) -> None:
    """
    Create or replace the crypto_prices table in DuckDB with the given DataFrame.

    Args:
        df: Cleaned Polars DataFrame.
        db_path: Path to DuckDB file (default from config).

    Raises:
        TransformationError: On DB connection or write failure.
    """
    path = Path(db_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.exception("Cannot create DB directory %s: %s", path.parent, e)
        raise TransformationError(f"Cannot create DB directory: {e}") from e

    try:
        conn = duckdb.connect(str(path))
        conn.register("prices_df", df.to_pandas())
        conn.execute(
            """
            CREATE OR REPLACE TABLE crypto_prices AS
            SELECT * FROM prices_df
        """
        )
        conn.close()
        logger.info("Loaded %d rows into %s", len(df), path)
    except Exception as e:
        logger.exception("DuckDB load failed: %s", e)
        raise TransformationError(f"DuckDB load failed: {e}") from e


def run_transformation(
    raw_dir: Path | None = None,
    db_path: str = DB_PATH,
) -> pl.DataFrame:
    """
    Run full transform + load: read latest raw → transform → load to DuckDB.

    Args:
        raw_dir: Raw data directory (default from config).
        db_path: DuckDB database path.

    Returns:
        The transformed Polars DataFrame (before load).

    Raises:
        TransformationError: If read, transform, or load fails.
    """
    logger.info("Starting transformation")
    get_data_dir()
    df = read_raw_latest(raw_dir=raw_dir)
    if df.is_empty():
        logger.warning("No raw data to transform")
        return df
    df = transform(df)
    load_to_duckdb(df, db_path=db_path)
    logger.info("Transformation complete")
    return df


if __name__ == "__main__":
    run_transformation()
