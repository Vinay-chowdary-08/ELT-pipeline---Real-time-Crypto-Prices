"""
Ingestion: fetch real-time crypto prices from CoinGecko API and store raw data.

This module is the Extract step of the ELT pipeline. It writes raw JSON/Parquet
to data/raw for idempotent reprocessing. Run as:
    python -m src.ingestion.main
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.config import (
    COINGECKO_BASE_URL,
    DEFAULT_COINS,
    DEFAULT_CURRENCY,
    RAW_DATA_DIR,
    get_data_dir,
)
from src.utils.logging import get_logger

load_dotenv()

logger = get_logger(__name__)


class IngestionError(Exception):
    """Raised when ingestion fails (API, IO, or validation)."""

    pass


def fetch_crypto_prices(
    coin_ids: str = DEFAULT_COINS,
    vs_currency: str = DEFAULT_CURRENCY,
    base_url: str = COINGECKO_BASE_URL,
) -> list[dict]:
    """
    Fetch simple price data from CoinGecko (no API key required for basic use).

    Args:
        coin_ids: Comma-separated coin IDs (e.g. 'bitcoin,ethereum,solana').
        vs_currency: Target currency (e.g. 'usd').
        base_url: CoinGecko API base URL.

    Returns:
        List of dicts with id, symbol, name, current_price, etc.

    Raises:
        IngestionError: On HTTP, connection, or JSON decode errors.
    """
    url = f"{base_url}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": coin_ids,
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
    }
    try:
        logger.info("Fetching crypto prices from %s (coins=%s)", url, coin_ids)
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise IngestionError(f"API returned non-list payload: type={type(data)}")
        logger.info("Fetched %d coins", len(data))
        return data
    except requests.RequestException as e:
        logger.exception("Request failed: %s", e)
        raise IngestionError(f"API request failed: {e}") from e
    except (ValueError, TypeError) as e:
        logger.exception("Invalid JSON or response: %s", e)
        raise IngestionError(f"Invalid API response: {e}") from e


def save_raw(payload: list[dict], output_dir: Path | None = None) -> Path:
    """
    Persist raw API response to a timestamped JSON file for replayability.

    Args:
        payload: List of coin objects from CoinGecko.
        output_dir: Directory for raw files (default: config RAW_DATA_DIR).

    Returns:
        Path to the written file.

    Raises:
        IngestionError: On IO or serialization errors.
    """
    output_dir = output_dir or RAW_DATA_DIR
    output_dir = Path(output_dir)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.exception("Failed to create output dir %s: %s", output_dir, e)
        raise IngestionError(f"Cannot create output directory: {e}") from e

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"crypto_prices_{ts}.json"
    try:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info("Saved raw data to %s (%d records)", path, len(payload))
        return path
    except (OSError, TypeError, ValueError) as e:
        logger.exception("Failed to write %s: %s", path, e)
        raise IngestionError(f"Failed to write raw file: {e}") from e


def run_ingestion(
    coin_ids: str = DEFAULT_COINS,
    vs_currency: str = DEFAULT_CURRENCY,
    output_dir: Path | None = None,
) -> Path:
    """
    Run the full ingestion step: fetch from API and save raw data.

    Args:
        coin_ids: Comma-separated CoinGecko coin IDs.
        vs_currency: Quote currency.
        output_dir: Where to write raw files (default from config).

    Returns:
        Path to the saved raw file.

    Raises:
        IngestionError: If fetch or save fails.
    """
    logger.info("Starting ingestion (currency=%s)", vs_currency)
    get_data_dir()
    data = fetch_crypto_prices(coin_ids=coin_ids, vs_currency=vs_currency)
    path = save_raw(data, output_dir=output_dir)
    logger.info("Ingestion complete: %s", path)
    return path


if __name__ == "__main__":
    run_ingestion()
