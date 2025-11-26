# Data Lake Layout

- `bronze/`: Raw API responses (Parquet), append-only.
- `silver/`: Cleaned, feature-engineered tables with target.
- `gold/`: Final model-ready datasets and trained artifacts.
- `interim/`: Temporary exports/checkpoints.
- `external/`: Third-party data used to enrich features.

