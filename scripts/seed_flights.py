#!/usr/bin/env python3
"""Seed DynamoDB io-travel-flights table from flights.parquet.

Reads from S3 by default, falls back to a local file for development.
Generates deterministic flight_id values so re-runs are idempotent.

Usage:
    python3 seed_flights.py              # seed from S3
    python3 seed_flights.py --dry-run    # preview first 5 items
    python3 seed_flights.py --local /path/to/flights.parquet  # use local file
"""

import argparse
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pandas as pd

S3_BUCKET = "enceladus-356364570033-us-west-2-an"
S3_KEY = "io/travel/flights/flights.parquet"
TABLE_NAME = "io-travel-flights"
REGION = "us-west-2"

# Columns that are entirely null in the source data — omit from DynamoDB
EXCLUDE_COLUMNS = {"discount_1", "discount_2", "discount_source"}


def generate_flight_id(date_iso: str, origin: str, dest: str, sequence: str, row_index: int) -> str:
    """Deterministic flight_id from composite key fields + row index tiebreaker."""
    raw = f"{date_iso}|{origin}|{dest}|{sequence}|{row_index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def read_parquet_from_s3() -> pd.DataFrame:
    """Read Parquet from S3."""
    s3 = boto3.client("s3", region_name=REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def read_parquet_local(path: str) -> pd.DataFrame:
    """Read Parquet from local file."""
    return pd.read_parquet(path)


def transform_row(row: pd.Series, now_iso: str, row_index: int = 0) -> dict:
    """Transform a Parquet row into a DynamoDB item."""
    # Build date fields
    date_val = row.get("date")
    if pd.isna(date_val):
        date_iso = ""
        date_yyyy_mm = ""
    else:
        dt = pd.Timestamp(date_val)
        date_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_yyyy_mm = dt.strftime("%Y-%m")

    # Deterministic ID
    origin = str(row.get("origin", "")) if pd.notna(row.get("origin")) else ""
    dest = str(row.get("dest", "")) if pd.notna(row.get("dest")) else ""
    seq = str(row.get("sequence", "0")) if pd.notna(row.get("sequence")) else "0"
    flight_id = generate_flight_id(date_iso, origin, dest, seq, row_index)

    item = {
        "flight_id": flight_id,
        "date_iso": date_iso,
        "date_yyyy_mm": date_yyyy_mm,
        "status": "active",
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    # Add all source columns, skipping excluded and null values
    for col in row.index:
        if col in EXCLUDE_COLUMNS:
            continue
        if col == "date":
            continue  # already handled as date_iso / date_yyyy_mm
        val = row[col]
        if pd.isna(val):
            continue

        # Type conversions for DynamoDB
        # Check bool before int — isinstance(True, int) is True in Python
        if isinstance(val, (bool,)):
            item[col] = bool(val)
        elif isinstance(val, pd.Timestamp):
            item[col] = val.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(val, float):
            item[col] = Decimal(str(round(val, 2)))
        elif isinstance(val, (int,)):
            item[col] = int(val)
        else:
            item[col] = str(val)

    return item


def seed(df: pd.DataFrame, dry_run: bool = False) -> dict:
    """Seed DynamoDB from a DataFrame. Returns summary stats."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    items = []
    for idx, row in df.iterrows():
        item = transform_row(row, now_iso, row_index=idx)
        items.append(item)

    if dry_run:
        print(f"DRY RUN: {len(items)} items would be written.\n")
        for i, item in enumerate(items[:5]):
            # Convert Decimals to float for readable JSON output
            printable = {}
            for k, v in item.items():
                if isinstance(v, Decimal):
                    printable[k] = float(v)
                else:
                    printable[k] = v
            print(f"--- Item {i + 1} ---")
            print(json.dumps(printable, indent=2, default=str))
            print()
        return {"total": len(items), "written": 0, "dry_run": True}

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    written = 0
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
            written += 1

    print(f"Seed complete: {written}/{len(items)} records written to {TABLE_NAME}")
    return {"total": len(items), "written": written, "dry_run": False}


def main():
    parser = argparse.ArgumentParser(description="Seed io-travel-flights DynamoDB table")
    parser.add_argument("--dry-run", action="store_true", help="Preview first 5 items without writing")
    parser.add_argument("--local", type=str, help="Path to local Parquet file (skips S3)")
    args = parser.parse_args()

    print(f"Reading Parquet data...")
    if args.local:
        print(f"  Source: local file {args.local}")
        df = read_parquet_local(args.local)
    else:
        print(f"  Source: s3://{S3_BUCKET}/{S3_KEY}")
        df = read_parquet_from_s3()

    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    result = seed(df, dry_run=args.dry_run)

    if not args.dry_run:
        # Verify count
        dynamodb = boto3.client("dynamodb", region_name=REGION)
        scan = dynamodb.scan(TableName=TABLE_NAME, Select="COUNT")
        count = scan["Count"]
        print(f"Verification: {count} records in {TABLE_NAME}")
        if count != result["total"]:
            print(f"WARNING: Expected {result['total']} but found {count}", file=sys.stderr)
            sys.exit(1)

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
