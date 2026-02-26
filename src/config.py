"""Load configuration from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
if not DATA_DIR.is_absolute():
    DATA_DIR = PROJECT_ROOT / DATA_DIR
_db_path = os.getenv("DB_PATH", str(DATA_DIR / "crypto_prices.duckdb"))
DB_PATH = _db_path if Path(_db_path).is_absolute() else str(PROJECT_ROOT / _db_path)
RAW_DATA_DIR = DATA_DIR / "raw"

# API
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
DEFAULT_COINS = os.getenv("COINS", "bitcoin,ethereum,solana,cardano").strip()
DEFAULT_CURRENCY = os.getenv("CURRENCY", "usd")


def get_data_dir() -> Path:
    """Ensure data directory exists and return it."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR
