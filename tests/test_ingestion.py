"""Tests for the ingestion module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.main import fetch_crypto_prices, run_ingestion, save_raw


def test_save_raw(sample_crypto_payload: list[dict], raw_dir: Path) -> None:
    """save_raw writes valid JSON and returns the file path."""
    path = save_raw(sample_crypto_payload, output_dir=raw_dir)
    assert path.exists()
    assert path.suffix == ".json"
    data = json.loads(path.read_text())
    assert len(data) == 2
    assert data[0]["id"] == "bitcoin"


@patch("src.ingestion.main.requests.get")
def test_fetch_crypto_prices(mock_get: MagicMock, sample_crypto_payload: list[dict]) -> None:
    """fetch_crypto_prices returns parsed JSON from API."""
    mock_get.return_value.json.return_value = sample_crypto_payload
    mock_get.return_value.raise_for_status = MagicMock()

    result = fetch_crypto_prices(coin_ids="bitcoin,ethereum", vs_currency="usd")
    assert len(result) == 2
    assert result[0]["symbol"] == "btc"
    mock_get.return_value.raise_for_status.assert_called_once()


def test_run_ingestion(raw_dir: Path, sample_crypto_payload: list[dict]) -> None:
    """run_ingestion with mocked fetch writes raw file."""
    with (
        patch("src.ingestion.main.get_data_dir"),
        patch("src.ingestion.main.fetch_crypto_prices", return_value=sample_crypto_payload),
    ):
        path = run_ingestion(output_dir=raw_dir)
    assert path.exists()
    assert "crypto_prices_" in path.name
