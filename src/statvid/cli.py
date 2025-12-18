"""Command-line interface entrypoint."""
from __future__ import annotations

import argparse
from .logging_config import configure_logging
# from .pipelines.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="StatVid ML pipeline CLI")
    sub = parser.add_subparsers(dest="command")

    p_run = sub.add_parser("run", help="Run end-to-end pipeline")
    p_run.add_argument("--limit", type=int, default=100, help="Max videos to ingest")
    p_run.add_argument("--model", type=str, default="ridge", choices=["ridge", "lightgbm"])
    p_run.add_argument("--dry-run", action="store_true", help="Plan without writing outputs")

    sub.add_parser("ingest", help="Only run ingestion")
    sub.add_parser("features", help="Only run feature engineering")
    sub.add_parser("train", help="Only train models")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging()

#    if args.command == "run":
#        run_pipeline(limit=args.limit, model=args.model, dry_run=args.dry_run)
    if args.command == "ingest":
        # Placeholder: call ingestion step
        pass
    elif args.command == "features":
        # Placeholder: call feature step
        pass
    elif args.command == "train":
        # Placeholder: call train step
        pass
    else:
        parser.print_help()

