"""
Generate correlation matrix and trend line figures from processed DuckDB data.

Run after the pipeline has loaded data: make run && make figures
Outputs: docs/correlation_matrix.png, docs/trend_line.png

Run from project root with PYTHONPATH=. (e.g. make figures).
"""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np

from src.config import DB_PATH

ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
NUM_TOP_COINS = 10  # For trend chart


def load_data():
    """Load crypto_prices from DuckDB into a pandas DataFrame."""
    path = Path(DB_PATH)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB not found at {path}. Run the pipeline first: make run")
    conn = duckdb.connect(str(path), read_only=True)
    df = conn.execute("SELECT * FROM crypto_prices").fetchdf()
    conn.close()
    if df.empty:
        raise ValueError("Table crypto_prices is empty. Run the pipeline first.")
    return df


def plot_correlation_matrix(df, out_path: Path) -> None:
    """Plot correlation matrix of numeric columns and save to out_path."""
    numeric = df.select_dtypes(include=[np.number])
    if numeric.shape[1] < 2:
        # Not enough columns; create a minimal placeholder
        fig, ax = plt.subplots(figsize=(6, 5))
        ax.text(0.5, 0.5, "Not enough numeric columns", ha="center", va="center")
        fig.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        return
    corr = numeric.corr()
    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(corr, cmap="RdYlBu_r", vmin=-1, vmax=1, aspect="auto")
    ax.set_xticks(range(len(corr.columns)))
    ax.set_yticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right")
    ax.set_yticklabels(corr.columns)
    plt.colorbar(im, ax=ax, label="Correlation")
    ax.set_title("Correlation matrix (numeric columns)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


def plot_trend_line(df, out_path: Path, top_n: int = NUM_TOP_COINS) -> None:
    """Plot top N coins by market cap (bar chart) as trend-style viz."""
    # Ensure we have required columns
    if "market_cap" not in df.columns or "symbol" not in df.columns:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.text(0.5, 0.5, "Missing market_cap or symbol", ha="center", va="center")
        fig.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        return
    top = df.nlargest(top_n, "market_cap")
    symbols = top["symbol"].str.upper()
    caps = top["market_cap"] / 1e9  # billions
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(range(len(symbols)), caps, color="steelblue", alpha=0.85)
    ax.set_yticks(range(len(symbols)))
    ax.set_yticklabels(symbols, fontsize=10)
    ax.set_xlabel("Market cap (USD billions)")
    ax.set_title(f"Top {top_n} coins by market cap")
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    df = load_data()
    plot_correlation_matrix(df, DOCS_DIR / "correlation_matrix.png")
    plot_trend_line(df, DOCS_DIR / "trend_line.png")
    print(f"Figures saved to {DOCS_DIR}")


if __name__ == "__main__":
    main()
