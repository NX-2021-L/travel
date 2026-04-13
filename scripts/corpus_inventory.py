#!/usr/bin/env python3
"""
corpus_inventory.py — Corpus mining Phase 1: Inventory & flight-email catalog.

Reads from s3://jreesegpt-private/corpus-raw/, scans MBOX files for
flight-related emails (headers only — no body content is read or logged),
analyzes iMessage CSV for flight keywords, and writes a structured JSON
inventory report to S3.

Part of FLY-TSK-011 (jreesegpt corpus mining plan).
"""

import argparse
import csv
import io
import json
import mailbox
import os
import re
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional

import boto3

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FLIGHT_SENDER_PATTERNS = [
    "alaskaair", "alaska airlines", "deltaairlines", "delta.com",
    "united.com", "aa.com", "americanairlines", "southwest",
    "jetblue", "spirit", "frontier", "hawaiianair",
    "expedia", "kayak", "google flights", "booking.com",
    "tripadvisor", "travelocity", "orbitz", "priceline",
    "hopper", "concur", "egencia", "amextravel",
]

FLIGHT_SUBJECT_PATTERNS = [
    "confirmation", "itinerary", "booking", "reservation",
    "flight", "e-ticket", "eticket", "boarding pass",
    "check-in", "travel", "receipt",
]

IMESSAGE_KEYWORDS = [
    "flight", "airport", "booked", "plane",
    "flew", "landed", "terminal", "gate", "boarding",
]


def progress(msg: str) -> None:
    """Print timestamped progress to stdout for SSM monitoring."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def list_objects(s3, bucket: str, prefix: str) -> List[dict]:
    """List all objects under a prefix, handling pagination."""
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"key": obj["Key"], "size_bytes": obj["Size"]})
    return objects


def download_to_temp(s3, bucket: str, key: str, tmpdir: str) -> str:
    """Download an S3 object to a local temp file and return the path."""
    local_path = os.path.join(tmpdir, os.path.basename(key))
    progress(f"  Downloading s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, local_path)
    progress(f"  Download complete ({os.path.getsize(local_path):,} bytes)")
    return local_path


# ---------------------------------------------------------------------------
# MBOX processing (headers only)
# ---------------------------------------------------------------------------

def _safe_header(msg, name: str) -> str:
    """Extract a header value, tolerant of encoding errors."""
    raw = msg.get(name, "")
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _extract_domain(from_header: str) -> str:
    """Pull the domain portion from a From header."""
    match = re.search(r"@([\w.-]+)", from_header)
    return match.group(1).lower() if match else from_header.lower().strip()


def _is_flight_sender(from_header: str) -> bool:
    lower = from_header.lower()
    return any(p in lower for p in FLIGHT_SENDER_PATTERNS)


def _is_flight_subject(subject: str) -> bool:
    lower = subject.lower()
    return any(p in lower for p in FLIGHT_SUBJECT_PATTERNS)


def _parse_date_safe(date_str: str) -> Optional[str]:
    """Best-effort parse of an email Date header to ISO-8601."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except Exception:
        return date_str.strip()


def process_mbox(local_path: str, source_file: str) -> dict:
    """
    Iterate an MBOX file, extracting only headers.
    Returns a dict with totals and flight-email catalog entries.
    """
    progress(f"  Opening MBOX: {source_file}")
    mbox = mailbox.mbox(local_path)

    total = 0
    flight_count = 0
    catalog_entries = []
    sender_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "dates": []})
    subject_pattern_counts: dict[str, int] = defaultdict(int)

    for msg in mbox:
        total += 1
        if total % 10000 == 0:
            progress(f"    ...processed {total:,} messages in {source_file}")

        from_hdr = _safe_header(msg, "From")
        to_hdr = _safe_header(msg, "To")
        subject = _safe_header(msg, "Subject")
        date_hdr = _safe_header(msg, "Date")

        sender_match = _is_flight_sender(from_hdr)
        subject_match = _is_flight_subject(subject)

        if sender_match or subject_match:
            flight_count += 1
            domain = _extract_domain(from_hdr)
            parsed_date = _parse_date_safe(date_hdr)

            catalog_entries.append({
                "sender_domain": domain,
                "subject": subject[:200],  # truncate very long subjects
                "date": parsed_date,
                "source_file": source_file,
            })

            sender_stats[domain]["count"] += 1
            if parsed_date:
                sender_stats[domain]["dates"].append(parsed_date)

            # Count which subject patterns matched
            lower_subj = subject.lower()
            for pat in FLIGHT_SUBJECT_PATTERNS:
                if pat in lower_subj:
                    subject_pattern_counts[pat] += 1

    mbox.close()
    progress(f"  MBOX complete: {source_file} — {total:,} total, {flight_count:,} flight-related")

    return {
        "total_messages": total,
        "flight_related_count": flight_count,
        "catalog_entries": catalog_entries,
        "sender_stats": dict(sender_stats),
        "subject_pattern_counts": dict(subject_pattern_counts),
    }


# ---------------------------------------------------------------------------
# CSV processing
# ---------------------------------------------------------------------------

def process_imessage_csv(local_path: str) -> dict:
    """Scan iMessage CSV for flight keywords. Reports counts only."""
    progress("  Processing imessage.csv")
    total_rows = 0
    flight_keyword_matches = 0
    keyword_freq: dict[str, int] = {kw: 0 for kw in IMESSAGE_KEYWORDS}

    with open(local_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header row
        for row in reader:
            total_rows += 1
            if total_rows % 100000 == 0:
                progress(f"    ...processed {total_rows:,} iMessage rows")
            # search all columns for keywords
            text = " ".join(row).lower()
            row_matched = False
            for kw in IMESSAGE_KEYWORDS:
                if kw in text:
                    keyword_freq[kw] += 1
                    row_matched = True
            if row_matched:
                flight_keyword_matches += 1

    progress(f"  iMessage complete: {total_rows:,} rows, {flight_keyword_matches:,} flight-keyword matches")
    return {
        "total_rows": total_rows,
        "flight_keyword_matches": flight_keyword_matches,
        "keyword_frequency": keyword_freq,
    }


def process_contacts_csv(local_path: str) -> dict:
    """Report column headers and row count for contacts.csv."""
    progress("  Processing contacts.csv")
    with open(local_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        row_count = sum(1 for _ in reader)
    progress(f"  contacts.csv: {row_count:,} rows, {len(header or [])} columns")
    return {
        "columns": header or [],
        "row_count": row_count,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def infer_format(key: str) -> str:
    lower = key.lower()
    if lower.endswith(".mbox"):
        return "mbox"
    elif lower.endswith(".csv"):
        return "csv"
    elif lower.endswith(".json"):
        return "json"
    elif lower.endswith(".gz") or lower.endswith(".tar.gz"):
        return "gzip"
    else:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Corpus inventory for flight data mining")
    parser.add_argument("--bucket", default="jreesegpt-private",
                        help="Source S3 bucket (default: jreesegpt-private)")
    parser.add_argument("--prefix", default="corpus-raw/",
                        help="S3 prefix for corpus files (default: corpus-raw/)")
    parser.add_argument("--output-bucket", default="devops-agentcli-compute",
                        help="Output S3 bucket (default: devops-agentcli-compute)")
    parser.add_argument("--output-key",
                        default="projects/travel/corpus-inventory/inventory-report.json",
                        help="Output S3 key for JSON report")
    parser.add_argument("--region", default="us-west-2",
                        help="AWS region (default: us-west-2)")
    args = parser.parse_args()

    progress("=" * 60)
    progress("Corpus Inventory — Phase 1")
    progress(f"Source: s3://{args.bucket}/{args.prefix}")
    progress(f"Output: s3://{args.output_bucket}/{args.output_key}")
    progress("=" * 60)

    s3 = boto3.client("s3", region_name=args.region)

    # Step 1: List all objects in corpus-raw/
    progress("Step 1: Listing corpus objects...")
    objects = list_objects(s3, args.bucket, args.prefix)
    progress(f"  Found {len(objects)} objects")

    file_inventory = []
    for obj in objects:
        fmt = infer_format(obj["key"])
        file_inventory.append({
            "path": f"s3://{args.bucket}/{obj['key']}",
            "size_bytes": obj["size_bytes"],
            "format": fmt,
            "total_messages": None,
            "flight_related_count": None,
        })

    # Step 2: Process each file
    all_catalog_entries = []
    all_sender_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "dates": []})
    all_subject_pattern_counts: dict[str, int] = defaultdict(int)
    imessage_analysis = None
    contacts_analysis = None

    with tempfile.TemporaryDirectory(prefix="corpus_inv_") as tmpdir:
        progress(f"Step 2: Processing files (temp dir: {tmpdir})")

        for i, obj in enumerate(objects):
            key = obj["key"]
            basename = os.path.basename(key)
            fmt = infer_format(key)

            progress(f"\nFile {i+1}/{len(objects)}: {basename} ({obj['size_bytes']:,} bytes, {fmt})")

            if fmt == "mbox":
                local_path = download_to_temp(s3, args.bucket, key, tmpdir)
                result = process_mbox(local_path, basename)

                # Update inventory entry
                for entry in file_inventory:
                    if entry["path"] == f"s3://{args.bucket}/{key}":
                        entry["total_messages"] = result["total_messages"]
                        entry["flight_related_count"] = result["flight_related_count"]

                all_catalog_entries.extend(result["catalog_entries"])

                # Merge sender stats
                for domain, stats in result["sender_stats"].items():
                    all_sender_stats[domain]["count"] += stats["count"]
                    all_sender_stats[domain]["dates"].extend(stats["dates"])

                # Merge subject pattern counts
                for pat, cnt in result["subject_pattern_counts"].items():
                    all_subject_pattern_counts[pat] += cnt

                # Clean up downloaded file to free disk space
                try:
                    os.remove(local_path)
                    progress(f"  Cleaned up {local_path}")
                except OSError:
                    pass

            elif basename == "imessage.csv":
                local_path = download_to_temp(s3, args.bucket, key, tmpdir)
                imessage_analysis = process_imessage_csv(local_path)

                for entry in file_inventory:
                    if entry["path"] == f"s3://{args.bucket}/{key}":
                        entry["total_messages"] = imessage_analysis["total_rows"]

                try:
                    os.remove(local_path)
                except OSError:
                    pass

            elif basename == "contacts.csv":
                local_path = download_to_temp(s3, args.bucket, key, tmpdir)
                contacts_analysis = process_contacts_csv(local_path)

                for entry in file_inventory:
                    if entry["path"] == f"s3://{args.bucket}/{key}":
                        entry["total_messages"] = contacts_analysis["row_count"]

                try:
                    os.remove(local_path)
                except OSError:
                    pass

            else:
                progress(f"  Skipping {basename} (unsupported format: {fmt})")

    # Step 3: Build sender pattern catalog with date ranges
    progress("\nStep 3: Building catalogs...")
    sender_pattern_catalog = {}
    for domain, stats in all_sender_stats.items():
        dates = sorted([d for d in stats["dates"] if d])
        sender_pattern_catalog[domain] = {
            "count": stats["count"],
            "date_range": [dates[0], dates[-1]] if dates else [],
        }

    # Overall date range
    all_dates = sorted([e["date"] for e in all_catalog_entries if e.get("date")])
    date_range = {
        "earliest": all_dates[0] if all_dates else None,
        "latest": all_dates[-1] if all_dates else None,
    }

    # Estimate unique flights (rough heuristic: cluster by date, ~1 flight per day)
    unique_date_days = set()
    for entry in all_catalog_entries:
        if entry.get("date"):
            try:
                day = entry["date"][:10]  # YYYY-MM-DD
                unique_date_days.add(day)
            except Exception:
                pass
    estimated_unique_flights = len(unique_date_days)

    # Step 4: Assemble report
    progress("Step 4: Assembling report...")
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "corpus_path": f"s3://{args.bucket}/{args.prefix}",
        "file_inventory": file_inventory,
        "flight_email_catalog": all_catalog_entries,
        "sender_pattern_catalog": dict(
            sorted(sender_pattern_catalog.items(), key=lambda x: x[1]["count"], reverse=True)
        ),
        "subject_pattern_catalog": dict(
            sorted(all_subject_pattern_counts.items(), key=lambda x: x[1], reverse=True)
        ),
        "date_range": date_range,
        "imessage_analysis": imessage_analysis,
        "contacts_analysis": contacts_analysis,
        "summary": {
            "total_corpus_files": len(file_inventory),
            "total_corpus_size_bytes": sum(f["size_bytes"] for f in file_inventory),
            "total_flight_related_emails": len(all_catalog_entries),
            "estimated_unique_flights": estimated_unique_flights,
        },
    }

    # Step 5: Upload report to S3
    progress(f"Step 5: Uploading report to s3://{args.output_bucket}/{args.output_key}")
    report_json = json.dumps(report, indent=2, default=str)
    s3.put_object(
        Bucket=args.output_bucket,
        Key=args.output_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json",
    )
    progress(f"  Report uploaded ({len(report_json):,} bytes)")

    # Final summary
    progress("\n" + "=" * 60)
    progress("INVENTORY COMPLETE")
    progress(f"  Corpus files: {report['summary']['total_corpus_files']}")
    progress(f"  Total size: {report['summary']['total_corpus_size_bytes']:,} bytes")
    progress(f"  Flight-related emails: {report['summary']['total_flight_related_emails']}")
    progress(f"  Estimated unique flights: {report['summary']['estimated_unique_flights']}")
    progress(f"  Report: s3://{args.output_bucket}/{args.output_key}")
    progress("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
