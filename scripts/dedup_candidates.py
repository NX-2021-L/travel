#!/usr/bin/env python3
"""
dedup_candidates.py -- Corpus mining Phase 3: Deduplication & classification.

Reads 422 flight candidate records from S3 Parquet (Phase 2 output), compares
against 298 existing records in DynamoDB io-travel-flights, and classifies each
candidate as: net_new, exact_duplicate, near_match, or conflict.

Outputs a deduped Parquet (all 422 rows with classification columns) and a
JSON summary report to S3.

Part of FLY-TSK-013 (jreesegpt corpus mining plan).

Runs on EC2 -- reads from S3 + DynamoDB, writes to S3.
"""

import argparse
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# Attempt pyarrow import
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip3 install pyarrow", flush=True)
    sys.exit(1)

REGION = "us-west-2"


def progress(msg: str) -> None:
    """Print timestamped progress for SSM monitoring."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def scan_dynamo_table(table_name: str) -> pd.DataFrame:
    """Full scan of DynamoDB table, handling pagination and Decimal types."""
    progress(f"  Scanning DynamoDB table: {table_name}")
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(table_name)

    items = []
    scan_kwargs = {}
    page = 0

    while True:
        response = table.scan(**scan_kwargs)
        batch = response.get("Items", [])
        items.extend(batch)
        page += 1
        progress(f"    Page {page}: {len(batch)} items (total: {len(items)})")

        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    progress(f"  Scan complete: {len(items)} existing records")

    if not items:
        return pd.DataFrame()

    # Convert Decimal types to float
    cleaned = []
    for item in items:
        row = {}
        for k, v in item.items():
            if isinstance(v, Decimal):
                row[k] = float(v)
            else:
                row[k] = v
        cleaned.append(row)

    return pd.DataFrame(cleaned)


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read Parquet file from S3 into a DataFrame."""
    progress(f"  Reading Parquet from s3://{bucket}/{key}")
    s3 = boto3.client("s3", region_name=REGION)
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    progress(f"  Loaded {len(df)} candidate records, {len(df.columns)} columns")
    return df


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize_date(date_str) -> str:
    """
    Normalize a date string to YYYY-MM-DD (date only).
    Handles: '2024-03-17T00:00:00Z', '2024-03-17', None/NaN.
    Returns empty string if unparseable.
    """
    if pd.isna(date_str) or date_str is None or str(date_str).strip() == "":
        return ""
    s = str(date_str).strip()
    # Take just the date portion (first 10 chars of ISO format)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Try pandas parsing as fallback
    try:
        return pd.Timestamp(s).strftime("%Y-%m-%d")
    except Exception:
        return ""


def normalize_airline(airline) -> str:
    """Lowercase airline name for comparison. Returns '' for None/NaN."""
    if pd.isna(airline) or airline is None:
        return ""
    return str(airline).strip().lower()


def normalize_iata(code) -> str:
    """Strip whitespace from IATA code, uppercase. Returns '' for None/NaN."""
    if pd.isna(code) or code is None:
        return ""
    return str(code).strip().upper()


def normalize_confirmation(code) -> str:
    """Normalize confirmation code: strip, uppercase. Returns '' for None/NaN."""
    if pd.isna(code) or code is None:
        return ""
    return str(code).strip().upper()


def parse_date_obj(date_str: str):
    """Parse a YYYY-MM-DD string to a date object. Returns None on failure."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Dedup engine
# ---------------------------------------------------------------------------

def build_existing_indices(existing_df: pd.DataFrame) -> dict:
    """
    Build lookup indices from existing DynamoDB records for fast matching.
    Returns dict with various index structures.
    """
    indices = {
        # (date, origin, dest, airline_lower) -> [flight_ids]
        "exact_key": defaultdict(list),
        # confirmation_code -> [(flight_id, date, origin, dest)]
        "confirmation": {},
        # (origin, dest) -> [(flight_id, date_obj, date_str, airline)]
        "route": defaultdict(list),
        # date_str -> [(flight_id, origin, dest, airline)]
        "date": defaultdict(list),
        # airline_lower -> [(flight_id, date_str, origin, dest)]
        "airline": defaultdict(list),
    }

    for _, row in existing_df.iterrows():
        fid = str(row.get("flight_id", ""))
        date_norm = normalize_date(row.get("date_iso"))
        origin = normalize_iata(row.get("origin"))
        dest = normalize_iata(row.get("dest"))
        airline = normalize_airline(row.get("airline"))
        conf = normalize_confirmation(row.get("confirmation_code"))
        date_obj = parse_date_obj(date_norm)

        # Exact composite key index
        if date_norm and origin and dest and airline:
            key = (date_norm, origin, dest, airline)
            indices["exact_key"][key].append(fid)

        # Confirmation code index
        if conf:
            indices["confirmation"][conf] = {
                "flight_id": fid,
                "date": date_norm,
                "origin": origin,
                "dest": dest,
                "airline": airline,
            }

        # Route index (for near-match by +-1 day)
        if origin and dest:
            indices["route"][(origin, dest)].append({
                "flight_id": fid,
                "date_obj": date_obj,
                "date_str": date_norm,
                "airline": airline,
            })

        # Date index (for low-confidence matching)
        if date_norm:
            indices["date"][date_norm].append({
                "flight_id": fid,
                "origin": origin,
                "dest": dest,
                "airline": airline,
            })

        # Airline index (for date+airline matching when origin/dest missing)
        if airline:
            indices["airline"][airline].append({
                "flight_id": fid,
                "date_str": date_norm,
                "date_obj": date_obj,
                "origin": origin,
                "dest": dest,
            })

    return indices


def classify_candidate(row: pd.Series, indices: dict) -> dict:
    """
    Classify a single candidate against existing records.

    Returns dict with:
      - dedup_status: net_new | exact_duplicate | near_match | conflict
      - matched_existing_ids: comma-separated flight_ids
      - match_reason: human-readable explanation
      - confidence: high | medium | low
    """
    date_norm = normalize_date(row.get("date_iso"))
    origin = normalize_iata(row.get("origin"))
    dest = normalize_iata(row.get("dest"))
    airline = normalize_airline(row.get("airline"))
    conf = normalize_confirmation(row.get("confirmation_code"))
    date_obj = parse_date_obj(date_norm)

    matched_ids = []
    reasons = []

    # ----- Check 1: Confirmation code exact match -----
    if conf and conf in indices["confirmation"]:
        existing = indices["confirmation"][conf]
        ex_fid = existing["flight_id"]

        # Check if it's exact match or conflict
        fields_match = True
        diffs = []
        if origin and existing["origin"] and origin != existing["origin"]:
            fields_match = False
            diffs.append(f"origin: {origin} vs {existing['origin']}")
        if dest and existing["dest"] and dest != existing["dest"]:
            fields_match = False
            diffs.append(f"dest: {dest} vs {existing['dest']}")
        if date_norm and existing["date"] and date_norm != existing["date"]:
            fields_match = False
            diffs.append(f"date: {date_norm} vs {existing['date']}")

        if not fields_match:
            return {
                "dedup_status": "conflict",
                "matched_existing_ids": ex_fid,
                "match_reason": f"confirmation_code match ({conf}) but fields differ: {'; '.join(diffs)}",
                "confidence": "high",
            }
        else:
            return {
                "dedup_status": "exact_duplicate",
                "matched_existing_ids": ex_fid,
                "match_reason": f"confirmation_code exact match ({conf})",
                "confidence": "high",
            }

    # ----- Check 2: Exact composite key match -----
    if date_norm and origin and dest and airline:
        key = (date_norm, origin, dest, airline)
        if key in indices["exact_key"]:
            fids = indices["exact_key"][key]
            return {
                "dedup_status": "exact_duplicate",
                "matched_existing_ids": ",".join(fids),
                "match_reason": "date+origin+dest+airline match",
                "confidence": "high",
            }

    # ----- Check 3: Near match -- same route within +-1 day -----
    if origin and dest and date_obj:
        route_key = (origin, dest)
        if route_key in indices["route"]:
            for existing in indices["route"][route_key]:
                if existing["date_obj"] is None:
                    continue
                day_diff = abs((date_obj - existing["date_obj"]).days)
                if day_diff == 1:
                    matched_ids.append(existing["flight_id"])
                    reasons.append(
                        f"same route {origin}-{dest} +/-1 day "
                        f"(candidate: {date_norm}, existing: {existing['date_str']})"
                    )

        if matched_ids:
            return {
                "dedup_status": "near_match",
                "matched_existing_ids": ",".join(matched_ids),
                "match_reason": "; ".join(reasons),
                "confidence": "medium",
            }

    # ----- Check 4: Missing origin/dest -- try date+airline match -----
    if (not origin or not dest) and date_norm and airline:
        if airline in indices["airline"]:
            for existing in indices["airline"][airline]:
                if existing["date_str"] == date_norm:
                    matched_ids.append(existing["flight_id"])
                    reasons.append(
                        f"date+airline match ({date_norm}, {airline})"
                        f" [candidate missing origin/dest]"
                    )
        if matched_ids:
            return {
                "dedup_status": "near_match",
                "matched_existing_ids": ",".join(matched_ids),
                "match_reason": "; ".join(reasons),
                "confidence": "low",
            }

    # ----- Check 5: Missing origin/dest -- try date+airline +-1 day -----
    if (not origin or not dest) and date_obj and airline:
        if airline in indices["airline"]:
            for existing in indices["airline"][airline]:
                if existing["date_obj"] is None:
                    continue
                day_diff = abs((date_obj - existing["date_obj"]).days)
                if day_diff == 1:
                    matched_ids.append(existing["flight_id"])
                    reasons.append(
                        f"date+airline near match ({date_norm} vs {existing['date_str']}, {airline})"
                        f" [candidate missing origin/dest, +/-1 day]"
                    )
        if matched_ids:
            return {
                "dedup_status": "near_match",
                "matched_existing_ids": ",".join(matched_ids),
                "match_reason": "; ".join(reasons),
                "confidence": "low",
            }

    # ----- No match found -----
    confidence = "high" if (origin and dest and date_norm) else (
        "medium" if date_norm else "low"
    )
    return {
        "dedup_status": "net_new",
        "matched_existing_ids": "",
        "match_reason": "",
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def build_report(candidates_df: pd.DataFrame, existing_count: int) -> dict:
    """Build the structured JSON dedup report."""
    total = len(candidates_df)

    # Classification counts
    status_counts = candidates_df["dedup_status"].value_counts().to_dict()
    classification = {
        "net_new": int(status_counts.get("net_new", 0)),
        "exact_duplicate": int(status_counts.get("exact_duplicate", 0)),
        "near_match": int(status_counts.get("near_match", 0)),
        "conflict": int(status_counts.get("conflict", 0)),
    }

    # Net-new by year
    net_new = candidates_df[candidates_df["dedup_status"] == "net_new"].copy()
    net_new_by_year = {}
    if len(net_new) > 0:
        net_new["_year"] = net_new["date_iso"].apply(
            lambda x: str(x)[:4] if pd.notna(x) and str(x).strip() else "unknown"
        )
        year_counts = net_new["_year"].value_counts().to_dict()
        net_new_by_year = {str(k): int(v) for k, v in sorted(year_counts.items())}

    # Net-new by airline
    net_new_by_airline = {}
    if len(net_new) > 0:
        net_new["_airline"] = net_new["airline"].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else "unknown"
        )
        airline_counts = net_new["_airline"].value_counts().to_dict()
        net_new_by_airline = {str(k): int(v) for k, v in sorted(airline_counts.items(), key=lambda x: -x[1])}

    # Net-new confidence distribution
    net_new_confidence = {"high": 0, "medium": 0, "low": 0}
    if len(net_new) > 0:
        conf_counts = net_new["confidence"].value_counts().to_dict()
        for level in ("high", "medium", "low"):
            net_new_confidence[level] = int(conf_counts.get(level, 0))

    # Duplicate details (first 20)
    dupes = candidates_df[candidates_df["dedup_status"] == "exact_duplicate"]
    duplicate_details = []
    for _, row in dupes.head(20).iterrows():
        duplicate_details.append({
            "candidate_flight_id": _safe_str(row.get("flight_id")),
            "date": _safe_str(row.get("date_iso")),
            "origin": _safe_str(row.get("origin")),
            "dest": _safe_str(row.get("dest")),
            "airline": _safe_str(row.get("airline")),
            "matched_ids": _safe_str(row.get("matched_existing_ids")),
            "reason": _safe_str(row.get("match_reason")),
        })

    # Near match details (all)
    near = candidates_df[candidates_df["dedup_status"] == "near_match"]
    near_match_details = []
    for _, row in near.iterrows():
        near_match_details.append({
            "candidate_flight_id": _safe_str(row.get("flight_id")),
            "date": _safe_str(row.get("date_iso")),
            "origin": _safe_str(row.get("origin")),
            "dest": _safe_str(row.get("dest")),
            "airline": _safe_str(row.get("airline")),
            "confirmation_code": _safe_str(row.get("confirmation_code")),
            "matched_ids": _safe_str(row.get("matched_existing_ids")),
            "reason": _safe_str(row.get("match_reason")),
            "confidence": _safe_str(row.get("confidence")),
        })

    # Conflict details (all)
    conflicts = candidates_df[candidates_df["dedup_status"] == "conflict"]
    conflict_details = []
    for _, row in conflicts.iterrows():
        conflict_details.append({
            "candidate_flight_id": _safe_str(row.get("flight_id")),
            "date": _safe_str(row.get("date_iso")),
            "origin": _safe_str(row.get("origin")),
            "dest": _safe_str(row.get("dest")),
            "airline": _safe_str(row.get("airline")),
            "confirmation_code": _safe_str(row.get("confirmation_code")),
            "matched_ids": _safe_str(row.get("matched_existing_ids")),
            "reason": _safe_str(row.get("match_reason")),
        })

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_candidates": int(total),
        "existing_records": int(existing_count),
        "classification": classification,
        "net_new_by_year": net_new_by_year,
        "net_new_by_airline": net_new_by_airline,
        "net_new_confidence_distribution": net_new_confidence,
        "duplicate_details": duplicate_details,
        "near_match_details": near_match_details,
        "conflict_details": conflict_details,
    }


def _safe_str(val) -> str:
    """Convert a value to string, handling NaN/None gracefully."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if pd.isna(val):
        return ""
    return str(val)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Write a DataFrame to Parquet on S3."""
    progress(f"  Writing Parquet to s3://{bucket}/{key}")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    progress(f"  Parquet uploaded ({buf.tell():,} bytes, {len(df)} rows)")


def write_json_to_s3(data: dict, bucket: str, key: str) -> None:
    """Write a JSON report to S3."""
    progress(f"  Writing JSON to s3://{bucket}/{key}")
    body = json.dumps(data, indent=2, default=str)
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    progress(f"  JSON uploaded ({len(body):,} bytes)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Phase 3: Dedup flight candidates against existing DynamoDB records"
    )
    parser.add_argument("--candidates-bucket", required=True,
                        help="S3 bucket containing candidate Parquet")
    parser.add_argument("--candidates-key", required=True,
                        help="S3 key for candidate Parquet file")
    parser.add_argument("--existing-bucket", required=True,
                        help="S3 bucket containing existing flights Parquet")
    parser.add_argument("--existing-key", required=True,
                        help="S3 key for existing flights Parquet file")
    parser.add_argument("--output-bucket", required=True,
                        help="S3 bucket for output files")
    parser.add_argument("--output-prefix", required=True,
                        help="S3 prefix for output files (e.g., projects/travel/flight-candidates/deduped/)")
    args = parser.parse_args()

    # Normalize prefix
    output_prefix = args.output_prefix.rstrip("/") + "/"

    progress("=" * 60)
    progress("Flight Candidate Deduplication -- Phase 3")
    progress(f"  Candidates: s3://{args.candidates_bucket}/{args.candidates_key}")
    progress(f"  Existing: s3://{args.existing_bucket}/{args.existing_key}")
    progress(f"  Output: s3://{args.output_bucket}/{output_prefix}")
    progress("=" * 60)

    # Step 1: Load existing records from S3 Parquet
    progress("\nStep 1: Loading existing records from S3 Parquet...")
    existing_df = read_parquet_from_s3(args.existing_bucket, args.existing_key)
    existing_count = len(existing_df)
    progress(f"  Existing records loaded: {existing_count}")

    # Step 2: Load candidates from S3 Parquet
    progress("\nStep 2: Loading candidates from S3 Parquet...")
    candidates_df = read_parquet_from_s3(args.candidates_bucket, args.candidates_key)
    progress(f"  Candidate records loaded: {len(candidates_df)}")

    # Step 3: Build lookup indices from existing records
    progress("\nStep 3: Building lookup indices...")
    indices = build_existing_indices(existing_df)
    progress(f"  Index sizes:")
    progress(f"    exact_key: {len(indices['exact_key'])} composite keys")
    progress(f"    confirmation: {len(indices['confirmation'])} codes")
    progress(f"    route: {len(indices['route'])} routes")
    progress(f"    date: {len(indices['date'])} dates")
    progress(f"    airline: {len(indices['airline'])} airlines")

    # Step 4: Classify each candidate
    progress("\nStep 4: Classifying candidates...")
    results = []
    status_running = defaultdict(int)

    for idx, (_, row) in enumerate(candidates_df.iterrows()):
        result = classify_candidate(row, indices)
        results.append(result)
        status_running[result["dedup_status"]] += 1

        if (idx + 1) % 100 == 0 or (idx + 1) == len(candidates_df):
            progress(f"  Classified {idx + 1}/{len(candidates_df)}: "
                     f"net_new={status_running['net_new']}, "
                     f"exact_dup={status_running['exact_duplicate']}, "
                     f"near={status_running['near_match']}, "
                     f"conflict={status_running['conflict']}")

    # Add classification columns to candidates DataFrame
    candidates_df["dedup_status"] = [r["dedup_status"] for r in results]
    candidates_df["matched_existing_ids"] = [r["matched_existing_ids"] for r in results]
    candidates_df["match_reason"] = [r["match_reason"] for r in results]
    candidates_df["confidence"] = [r["confidence"] for r in results]

    # Step 5: Write deduped Parquet to S3
    progress("\nStep 5: Writing deduped Parquet...")
    parquet_key = f"{output_prefix}flight-candidates-deduped.parquet"
    write_parquet_to_s3(candidates_df, args.output_bucket, parquet_key)

    # Step 6: Build and write report
    progress("\nStep 6: Building dedup report...")
    report = build_report(candidates_df, existing_count)
    json_key = f"{output_prefix}dedup-report.json"
    write_json_to_s3(report, args.output_bucket, json_key)

    # Print summary
    progress("\n" + "=" * 60)
    progress("DEDUPLICATION COMPLETE")
    progress(f"  Total candidates: {report['total_candidates']}")
    progress(f"  Existing records: {report['existing_records']}")
    progress(f"  Classification:")
    for status, count in report["classification"].items():
        pct = (count / report["total_candidates"] * 100) if report["total_candidates"] else 0
        progress(f"    {status}: {count} ({pct:.1f}%)")
    progress(f"  Net-new confidence: {report['net_new_confidence_distribution']}")
    progress(f"  Near matches for review: {len(report['near_match_details'])}")
    progress(f"  Conflicts for review: {len(report['conflict_details'])}")
    progress(f"\n  Output:")
    progress(f"    Parquet: s3://{args.output_bucket}/{parquet_key}")
    progress(f"    Report: s3://{args.output_bucket}/{json_key}")
    progress("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
