"""
Streamlit dashboard: visualize crypto prices from DuckDB.

Run with: streamlit run src/app/dashboard.py
"""

import streamlit as st

from src.config import DB_PATH, get_data_dir


def main() -> None:
    """Render the crypto prices dashboard."""
    st.set_page_config(page_title="Crypto Prices", page_icon="📈", layout="wide")
    st.title("Real-time Crypto Prices")
    st.caption("ELT Pipeline: CoinGecko → Polars → DuckDB")

    get_data_dir()
    from pathlib import Path

    db = Path(DB_PATH)
    if not db.exists():
        st.warning(
            "No database yet. Run the pipeline: `make run` or `make ingest` then `make transform`."
        )
        return

    import duckdb

    conn = duckdb.connect(str(db), read_only=True)
    df = conn.execute("SELECT * FROM crypto_prices ORDER BY market_cap_rank NULLS LAST").fetchdf()
    conn.close()

    if df.empty:
        st.info("Table is empty. Run ingestion and transformation first.")
        return

    st.dataframe(df, use_container_width=True)
    st.metric("Coins loaded", len(df))


if __name__ == "__main__":
    main()
