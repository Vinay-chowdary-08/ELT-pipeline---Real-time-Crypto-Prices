# 📈 Real-Time Crypto Prices — ELT Pipeline

**Production-grade ELT pipeline** that ingests live cryptocurrency market data, transforms it with high-performance Polars, loads it into DuckDB, and serves a Streamlit dashboard. Built to demonstrate scalable data engineering practices for analytics-ready datasets.

---

## 🎯 Business Impact

| Outcome | Why It Matters |
|--------|-----------------|
| **Single source of truth** | Raw API responses are stored as-is (`data/raw/`), enabling reprocessing and auditability without re-calling external APIs. |
| **Analytics-ready data** | Cleaned, typed data in DuckDB supports fast ad-hoc SQL, BI tools, and downstream ML—reducing time-to-insight from hours to minutes. |
| **Operational reliability** | Modular Extract → Transform → Load with logging and error handling makes the pipeline debuggable and safe to run on a schedule (e.g. cron or Airflow). |
| **Cost & performance** | Polars and DuckDB keep processing local and fast; minimal cloud dependency and no unnecessary data movement. |

This project mirrors real-world patterns: **ingest from APIs → validate & transform → load into a queryable store → expose via app**, with tests and CI to keep quality high.

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|------------|
| **Extract** | [![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](#) · [![Requests](https://img.shields.io/badge/Requests-HTTP-FF6B6B)](#) · **CoinGecko API** |
| **Transform** | [![Polars](https://img.shields.io/badge/Polars-Fast%20DataFrames-CD7928?logo=rust)](#) |
| **Load** | [![DuckDB](https://img.shields.io/badge/DuckDB-In-Process%20Analytics-000000?logo=duckdb)](#) |
| **App** | [![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](#) |
| **Quality** | [![pytest](https://img.shields.io/badge/pytest-Tests-0A9EDC?logo=pytest)](#) · [![Ruff](https://img.shields.io/badge/Ruff-Lint-FFCC49)](#) |
| **Ops** | **Docker** · **GitHub Actions** · **Makefile** |

*Python · Polars · DuckDB · Streamlit · pytest · Ruff · Docker · CI/CD*

---

## 📁 Project Structure

```
elt-pipeline/
├── data/                    # Gitignored: raw JSON + DuckDB
├── docs/                    # Generated figures for README
├── notebooks/               # Exploratory analysis
├── scripts/                 # One-off scripts (e.g. generate figures)
├── src/
│   ├── config.py            # Env-based configuration
│   ├── ingestion/           # Extract: API → raw files
│   ├── transformation/     # Transform + Load: Polars → DuckDB
│   └── app/                 # Streamlit dashboard
├── tests/                   # Unit + data quality tests
├── .github/workflows/       # CI: ruff + pytest
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── requirements.txt
```

---

## 🚀 How to Run

### Prerequisites

- **Python 3.10+**
- **pip** or **uv** (optional, for faster installs)

### 1. Clone and set up environment

```bash
git clone <your-repo-url>
cd elt-pipeline
cp .env.example .env   # optional: set COINGECKO_API_KEY for higher rate limits
make setup
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
```

### 2. Run the pipeline

```bash
make run
```

This runs **ingest** (fetch from CoinGecko → save to `data/raw/`) then **transform** (Polars clean → load into DuckDB).

**Step-by-step:**

```bash
make ingest    # Fetch and save raw data
make transform # Transform and load into DuckDB
```

### 3. Open the dashboard

```bash
make app
```

Then open **http://localhost:8501** in your browser.

### 4. Generate README figures (optional)

Requires existing data in DuckDB (run the pipeline first).

```bash
make figures   # Writes docs/correlation_matrix.png and docs/trend_line.png
```

---

## 📋 Makefile Commands

| Command | Description |
|---------|-------------|
| `make setup` | Create venv and install dependencies |
| `make run` | Full pipeline: ingest → transform |
| `make ingest` | Fetch from API and save raw JSON |
| `make transform` | Transform and load into DuckDB |
| `make app` | Start Streamlit dashboard |
| `make test` | Run pytest (unit + data quality) |
| `make lint` | Ruff check + format check |
| `make figures` | Generate correlation matrix and trend line images |
| `make docker-up` | Build and run with Docker Compose |

---

## 🐳 Docker

```bash
docker compose up -d --build           # Run pipeline once
docker compose --profile dashboard up # Pipeline + Streamlit
```

---

## ✅ CI/CD

On every push/PR to `main` or `master`, GitHub Actions runs:

- **Ruff** — lint and format check  
- **pytest** — unit tests and data-quality tests  

---

## 📊 Sample Output

After running the pipeline, you can inspect data locally:

```bash
# DuckDB CLI (if installed)
duckdb data/crypto_prices.duckdb "SELECT id, symbol, current_price, market_cap_rank FROM crypto_prices ORDER BY market_cap_rank LIMIT 5"
```

**Example figures** (generated by `make figures`):

| Correlation matrix (numeric fields) | Trend: top coins by market cap |
|-------------------------------------|---------------------------------|
| ![Correlation matrix](docs/correlation_matrix.png) | ![Trend line](docs/trend_line.png) |

*Run `make run` then `make figures` to generate these images in `docs/`.*

---

## ⚙️ Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `COINGECKO_API_KEY` | Optional; higher rate limits | *(empty)* |
| `DATA_DIR` | Base data directory | `data` |
| `DB_PATH` | DuckDB file path | `data/crypto_prices.duckdb` |
| `COINS` | Comma-separated coin IDs | `bitcoin,ethereum,solana,cardano` |
| `CURRENCY` | Quote currency | `usd` |

---

## 📜 License

MIT
# ELT-pipeline---Real-time-Crypto-Prices
