"""Tests for the transformation module."""

from pathlib import Path

import polars as pl

from src.transformation.main import load_to_duckdb, read_raw_latest, transform


def test_read_raw_latest_empty(tmp_path: Path) -> None:
    """read_raw_latest returns empty DataFrame when no raw files exist."""
    df = read_raw_latest(raw_dir=tmp_path)
    assert df.is_empty()


def test_read_raw_latest(raw_json_file: Path) -> None:
    """read_raw_latest reads the most recent JSON file."""
    df = read_raw_latest(raw_dir=raw_json_file.parent)
    assert len(df) == 2
    assert "current_price" in df.columns
    assert df["id"].to_list() == ["bitcoin", "ethereum"]


def test_transform(sample_crypto_payload: list[dict]) -> None:
    """transform returns DataFrame with correct dtypes."""
    df = pl.DataFrame(sample_crypto_payload)
    out = transform(df)
    assert out.shape[0] == 2
    assert out["current_price"].dtype == pl.Float64


def test_load_to_duckdb(sample_crypto_payload: list[dict], tmp_path: Path) -> None:
    """load_to_duckdb creates a table and persists data."""
    import duckdb

    df = pl.DataFrame(sample_crypto_payload)
    db_path = str(tmp_path / "test.duckdb")
    load_to_duckdb(df, db_path=db_path)
    conn = duckdb.connect(db_path, read_only=True)
    rows = conn.execute("SELECT id, current_price FROM crypto_prices").fetchall()
    conn.close()
    assert len(rows) == 2
    assert rows[0][0] == "bitcoin"
