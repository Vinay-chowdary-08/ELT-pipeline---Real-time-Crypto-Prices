"""Pytest fixtures for ELT pipeline tests."""

import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_crypto_payload() -> list[dict]:
    """Minimal CoinGecko-style response for testing."""
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 43250.0,
            "market_cap": 850_000_000_000,
            "total_volume": 25_000_000_000,
            "high_24h": 44000.0,
            "low_24h": 42800.0,
            "price_change_24h": 200.0,
            "price_change_percentage_24h": 0.47,
            "market_cap_rank": 1,
            "last_updated": "2024-01-15T12:00:00.000Z",
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2650.0,
            "market_cap": 320_000_000_000,
            "total_volume": 12_000_000_000,
            "high_24h": 2700.0,
            "low_24h": 2600.0,
            "price_change_24h": -50.0,
            "price_change_percentage_24h": -1.85,
            "market_cap_rank": 2,
            "last_updated": "2024-01-15T12:00:00.000Z",
        },
    ]


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    """Temporary directory for raw data in tests."""
    d = tmp_path / "raw"
    d.mkdir()
    return d


@pytest.fixture
def raw_json_file(raw_dir: Path, sample_crypto_payload: list[dict]) -> Path:
    """Write sample payload to a raw JSON file and return its path."""
    path = raw_dir / "crypto_prices_20240115_120000.json"
    path.write_text(json.dumps(sample_crypto_payload))
    return path
