#!/usr/bin/env python
"""
Download a single month of NYC Yellow Taxi trip data into data/raw/.

Usage:
    python scripts/extract_data.py --month 2024-03 [--filetype parquet] [--replace]
"""

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple
import requests
from tqdm import tqdm

# Base URL patterns for each supported filetype
BASE_URLS = {
    "parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet",
    "csv": "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_{year}-{month:02d}.csv",
    "csv.gz": "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_{year}-{month:02d}.csv.gz",
}

RAW_DIR = Path("data/raw")


def build_url(yyyymm: dt.date, filetype: str) -> str:
    """Return the full download URL for the given month/filetype."""
    if filetype not in BASE_URLS:
        raise ValueError(f"Unsupported filetype '{filetype}'. Choose from {list(BASE_URLS.keys())}.")
    return BASE_URLS[filetype].format(year=yyyymm.year, month=yyyymm.month)


def head_size(url: str) -> Optional[int]:
    """Fetch content-length via HEAD; return None if unavailable."""
    try:
        resp = requests.head(url, timeout=10)
        resp.raise_for_status()
        size_str = resp.headers.get("Content-Length")
        return int(size_str) if size_str else None
    except Exception:
        return None


def download(url: str, dest: Path) -> Tuple[int, str]:
    """Stream download *url* into *dest*, returning (bytes_written, etag)."""
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        with tmp_path.open("wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=dest.name
        ) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
    tmp_path.replace(dest)
    return total, r.headers.get("ETag", "").strip("\"")


def parse_month(value: str) -> dt.date:
    """Parse YYYY-MM into a date object (first day of month)."""
    try:
        return dt.datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--month must be in YYYY-MM format") from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download NYC TLC yellow taxi data for a given month"
    )
    parser.add_argument("--month", required=True, type=parse_month, help="Month in YYYY-MM")
    parser.add_argument(
        "--filetype",
        default="parquet",
        choices=BASE_URLS.keys(),
        help="Filetype to download (default: parquet)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Re-download even if file already exists",
    )
    args = parser.parse_args()

    # Prepare paths & logging
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logfile = RAW_DIR / "extract_data.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(logfile), logging.StreamHandler(sys.stdout)],
    )
    log = logging.getLogger("extract_data")

    url = build_url(args.month, args.filetype)
    filename = f"yellow_tripdata_{args.month.strftime('%Y-%m')}.{args.filetype}"
    dest_path = RAW_DIR / filename

    log.info("Source URL: %s", url)
    remote_size = head_size(url)

    if dest_path.exists() and not args.replace:
        local_size = dest_path.stat().st_size
        if remote_size is None or local_size == remote_size:
            log.info("File already exists and matches remote size – skipping download.")
            print(dest_path)
            return
        log.info("Local file differs from remote – re-downloading.")

    try:
        size, etag = download(url, dest_path)
        log.info("Downloaded %s (%.2f MB)", dest_path, size / 1e6)
        meta = {
            "url": url,
            "size": size,
            "etag": etag,
            "downloaded_at": dt.datetime.utcnow().isoformat() + "Z",
        }
        meta_path = dest_path.with_suffix(dest_path.suffix + ".meta.json")
        meta_path.write_text(json.dumps(meta, indent=2))
        print(dest_path)
    except requests.HTTPError as exc:
        log.error("HTTP error while downloading %s: %s", url, exc)
        sys.exit(1)
    except Exception as exc:
        log.error("Unexpected error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
