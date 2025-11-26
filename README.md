# StatVid

Predict daily engagement (views_per_day) from YouTube metadata via a simple, production-grade ML pipeline.

## Goals
- Ingest YouTube Data API v3 metadata → Bronze (Parquet).
- Engineer features and target views_per_day → Silver.
- Train Ridge / LightGBM regression → Gold.

## Quickstart
1) Python 3.10+
2) `python -m venv .venv && source .venv/bin/activate` (or Windows equivalent)
3) `pip install -r requirements.txt`
4) Copy `.env.example` to `.env` and set `YOUTUBE_API_KEY`
5) CLI: `python -m statvid --help`

## Data Layers
- Bronze: raw API responses (Parquet)
- Silver: cleaned + features + target
- Gold: training-ready data + models

## Structure
See `src/statvid/` for modules and `data/` for lake layout.

## Docker
- `docker build -t statvid .`
- `docker run --rm -v $PWD/data:/app/data statvid --help`

## Tech Stack
**Languages & Environment:**
- Python 3.11
- Anaconda (environment management)
- Docker (containerization)
- GitHub (version control)

**Data Handling & Storage:**
- Pandas, NumPy (data processing)
- Parquet (dataset storage: bronze/silver/gold)
- DuckDB (local SQL analytics)

**Visualization:**
- Matplotlib + Seaborn

**Data Acquisition:**
- YouTube Data API v3
- requests
- python-dotenv
- pyarrow
- isodate

**Modeling:**
- scikit-learn (baseline models, evaluation)
- LightGBM (gradient boosting model)

**Future Enhancements:**
- MLflow (model tracking)
- Airflow (pipeline orchestration)
- AWS S3 + Databricks (cloud ETL + lakehouse)
- SHAP (explainability)
- FastAPI (model serving)
- Next.js + recharts/shadcn (dashboard frontend)