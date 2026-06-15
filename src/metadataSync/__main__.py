#! /usr/bin/env python3
"""
CLI entry point. Parses arguments, builds Config and Credentials, runs the pipeline.

Usage:
    python -m metadatasync --root /path/to/repo [--sync-runinfo] [--sync-biosample]
"""

import argparse
import asyncio
import logging
from pathlib import Path

from config import Config, Credentials
from orchestrator import MetadataSync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="metadatasync",
        description="Sync NCBI SRA and biosample metadata.",
    )

    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Repository root",
    )

    parser.add_argument(
        "--sync-runinfo",
        action="store_true",
        help="Fetch fresh runinfo from NCBI SRA",
    )

    parser.add_argument(
        "--sync-biosample",
        action="store_true",
        help="Fetch fresh biosample metadata from NCBI",
    )

    parser.add_argument(
        "--d1-upload",
        action="store_true",
        help="Upload the combined dataset to Cloudflare D1",
    )

    parser.add_argument(
        "--biosample-min-coverage",
        type=float,
        default=0.05,
        help="Minimum fraction of non-null values for a biosample column to be kept",
    )

    args = parser.parse_args()

    if not args.root.exists():
        parser.error(f"--root does not exist: {args.root}")

    if not args.root.is_dir():
        parser.error(f"--root is not a directory: {args.root}")

    env_file = args.root / ".env"

    if not env_file.exists():
        parser.error(f"Missing required .env file: {env_file}")

    if not (args.sync_runinfo or args.sync_biosample or args.d1_upload):
        parser.error(
            "At least one action must be specified: "
            "--sync-runinfo, --sync-biosample, or --d1-upload"
        )

    return args


def main():
    args = parse_args()

    from dotenv import load_dotenv

    load_dotenv(args.root / ".env", override=True)
    credentials = Credentials.from_env()

    config = Config(
        project_root=args.root,
        biosample_min_coverage=args.biosample_min_coverage,
    )

    pipeline = MetadataSync(
        config=config,
        credentials=credentials,
    )

    asyncio.run(
        pipeline.run(
            sync_runinfo=args.sync_runinfo,
            sync_biosample=args.sync_biosample,
            d1_upload=args.d1_upload,
        )
    )


if __name__ == "__main__":
    main()
