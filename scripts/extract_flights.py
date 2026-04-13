#!/usr/bin/env python3
"""
extract_flights.py -- Corpus mining Phase 2: Flight extraction & normalization.

Reads MBOX files from S3, extracts structured flight booking records from email
bodies, normalizes to DynamoDB-compatible schema, and writes Parquet + summary
JSON to S3.

Part of FLY-TSK-012 (jreesegpt corpus mining plan).

Runs on EC2 -- reads from s3://jreesegpt-private/corpus-raw/2025-09-14/,
outputs to s3://devops-agentcli-compute/projects/travel/flight-candidates/raw/.
"""

import argparse
import gc
import hashlib
import json
import mailbox
import os
import re
import resource
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple

import boto3

# ---------------------------------------------------------------------------
# Attempt pyarrow import -- install hint if missing
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip3 install pyarrow", flush=True)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MBOX_FILES = [
    "gmail-sent.mbox",
    "work-sent.mbox",
    "workspace-sent.mbox",
    # gmail-chat.mbox excluded -- Phase 1 found 0 flight hits
]

# Airline sender domain -> canonical name mapping
AIRLINE_DOMAINS = {
    "alaskaair.com": "Alaska Airlines",
    "ifly.alaskaair.com": "Alaska Airlines",
    "aa.com": "American Airlines",
    "delta.com": "Delta Air Lines",
    "deltaairlines.com": "Delta Air Lines",
    "united.com": "United Airlines",
    "southwest.com": "Southwest Airlines",
    "southwestairlines.com": "Southwest Airlines",
    "jetblue.com": "JetBlue Airways",
    "spirit.com": "Spirit Airlines",
    "frontier.com": "Frontier Airlines",
    "flyfrontier.com": "Frontier Airlines",
    "hawaiianairlines.com": "Hawaiian Airlines",
    "hawaiianair.com": "Hawaiian Airlines",
    "suncountry.com": "Sun Country Airlines",
    "allegiantair.com": "Allegiant Air",
    "virginamerica.com": "Virgin America",
    "virginatlantic.com": "Virgin Atlantic",
    "britishairways.com": "British Airways",
    "aircanada.com": "Air Canada",
    "westjet.com": "WestJet",
    "aeromexico.com": "Aeromexico",
    "copa.com": "Copa Airlines",
    "icelandair.com": "Icelandair",
    "condor.com": "Condor",
    "lufthansa.com": "Lufthansa",
}

# Travel agency/OTA domains (Tier 2)
OTA_DOMAINS = {
    "expedia.com": "Expedia",
    "kayak.com": "Kayak",
    "orbitz.com": "Orbitz",
    "priceline.com": "Priceline",
    "travelocity.com": "Travelocity",
    "hopper.com": "Hopper",
    "google.com": "Google Flights",
    "cheapoair.com": "CheapOAir",
}

# Flight number prefix -> airline
FLIGHT_NUMBER_AIRLINES = {
    "AS": "Alaska Airlines",
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue Airways",
    "NK": "Spirit Airlines",
    "F9": "Frontier Airlines",
    "HA": "Hawaiian Airlines",
    "SY": "Sun Country Airlines",
    "G4": "Allegiant Air",
    "VX": "Virgin America",
    "VS": "Virgin Atlantic",
    "BA": "British Airways",
    "AC": "Air Canada",
    "WS": "WestJet",
    "AM": "Aeromexico",
    "CM": "Copa Airlines",
    "FI": "Icelandair",
    "DE": "Condor",
    "LH": "Lufthansa",
}

# Tier 1 subject signal patterns (high confidence)
TIER1_SUBJECT_PATTERNS = [
    "confirmation", "itinerary", "e-ticket", "eticket",
    "receipt", "booking confirmation", "travel confirmation",
    "flight confirmation", "reservation confirmation",
]

# Confirmation code pattern: 6 alphanumeric chars (standard PNR)
CONFIRMATION_CODE_RE = re.compile(
    r'\b(?:confirmation\s*(?:code|number|#)?[\s:]*)?([A-Z][A-Z0-9]{5})\b'
)

# Flight number patterns: 2-letter prefix + 1-4 digit number
FLIGHT_NUMBER_RE = re.compile(
    r'\b(' + '|'.join(FLIGHT_NUMBER_AIRLINES.keys()) + r')[\s-]?(\d{1,4})\b'
)

# Fare/cost patterns
FARE_RE = re.compile(r'\$\s?([\d,]+\.?\d{0,2})')

# Date patterns for flight dates in email body
DATE_PATTERNS = [
    # "January 15, 2024" or "Jan 15, 2024"
    (re.compile(
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December'
        r'|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})\b',
        re.IGNORECASE
    ), "%B %d %Y"),
    # "01/15/2024" or "1/15/2024"
    (re.compile(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b'), "MDY"),
    # "2024-01-15"
    (re.compile(r'\b(\d{4})-(\d{2})-(\d{2})\b'), "ISO"),
    # "15 Jan 2024" or "15 January 2024"
    (re.compile(
        r'\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December'
        r'|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})\b',
        re.IGNORECASE
    ), "DMY"),
]

# Month name -> number
MONTH_MAP = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
    "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}

# IATA airport code context patterns -- we look for codes near these words
AIRPORT_CONTEXT_RE = re.compile(
    r'(?:depart(?:ing|ure|s)?|arriv(?:al|ing|e|es)?|from|to|origin|destination'
    r'|departing\s+city|arriving\s+city|flight\s+from|flight\s+to'
    r'|leaves?\s+from|arrives?\s+(?:at|in))\s*:?\s*'
    r'(?:\([A-Z]{3}\)\s*)?([A-Z]{3})\b',
    re.IGNORECASE
)

# Broader IATA pattern: 3 uppercase letters that appear as standalone tokens
# We validate against the IATA set below
IATA_STANDALONE_RE = re.compile(r'\b([A-Z]{3})\b')

# Route-like pattern: SEA-SFO, SEA to SFO, SEA -> SFO
ROUTE_RE = re.compile(
    r'\b([A-Z]{3})\s*(?:[-–—>]|to|->)\s*([A-Z]{3})\b'
)

# ---------------------------------------------------------------------------
# IATA airport code set (~400 common codes)
# Includes all major US airports + international hubs likely in a personal
# travel corpus spanning 2006-2025.
# ---------------------------------------------------------------------------

IATA_CODES = {
    # Major US airports
    "ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "SEA", "LAS", "MCO",
    "CLT", "EWR", "PHX", "IAH", "MIA", "BOS", "MSP", "FLL", "DTW", "PHL",
    "LGA", "BWI", "SLC", "SAN", "IAD", "DCA", "MDW", "TPA", "PDX", "HNL",
    "STL", "BNA", "AUS", "HOU", "OAK", "MSY", "RDU", "SJC", "SMF", "SNA",
    "MCI", "SAT", "PIT", "CLE", "CMH", "IND", "CVG", "BDL", "JAX", "ABQ",
    "ANC", "SDF", "RNO", "OMA", "MEM", "BUF", "OGG", "PBI", "TUS", "OKC",
    "ELP", "ONT", "BUR", "RSW", "RIC", "ORF", "BOI", "LIT", "GEG", "DSM",
    "KOA", "LIH", "ICT", "CHS", "DAY", "TUL", "GRR", "PVD", "MKE", "BHM",
    "SYR", "SAV", "PSP", "GSP", "HSV", "FAT", "COS", "ISP", "ROC", "ALB",
    "SRQ", "TYS", "XNA", "PNS", "DAB", "MSN", "LEX", "PWM", "MYR", "CAK",
    "BTR", "GSO", "FAI", "BZN", "FSD", "RAP", "AZO", "MOB", "SGF", "CRW",
    "AVL", "JNU", "GNV", "EUG", "MLB", "LBB", "AMA", "FNT", "SBN", "GRB",
    "BTV", "TLH", "CID", "LAN", "MFE", "SHV", "EVV", "FWA", "GPT", "MHT",
    "MLI", "TRI", "VPS", "PIH", "BIL", "JAC", "MTJ", "HDN", "EGE", "SUN",
    "STT", "STX", "SJU",
    # Alaska-relevant
    "AKN", "ADQ", "BET", "CDV", "DLG", "SIT", "YAK", "KTN", "WRG", "PSG",
    "OME", "OTZ", "BRW",
    # Canada
    "YVR", "YYC", "YEG", "YYZ", "YOW", "YUL", "YHZ", "YWG", "YXE", "YQR",
    "YLW", "YKA",
    # Mexico & Caribbean
    "CUN", "MEX", "GDL", "SJD", "PVR", "CZM", "MZT", "ACA", "NAS", "MBJ",
    "KIN", "GCM", "PUJ", "STI", "SDQ", "SXM", "AUA", "CUR", "BON", "BDA",
    # Central & South America
    "PTY", "SJO", "LIR", "GUA", "SAL", "TGU", "MGA", "BZE", "BOG", "MDE",
    "CLO", "CTG", "UIO", "GYE", "LIM", "CUZ", "SCL", "EZE", "GRU", "GIG",
    "BSB", "CNF", "SSA", "MVD", "ASU", "LPB", "VVI", "CCS",
    # Europe
    "LHR", "LGW", "STN", "MAN", "EDI", "GLA", "CDG", "ORY", "AMS", "FRA",
    "MUC", "BER", "HAM", "DUS", "FCO", "MXP", "VCE", "NAP", "MAD", "BCN",
    "LIS", "OPO", "ZRH", "GVA", "VIE", "PRG", "WAW", "BUD", "BRU", "CPH",
    "OSL", "ARN", "HEL", "DUB", "KEF", "ATH", "IST", "SAW",
    # Asia & Pacific
    "NRT", "HND", "KIX", "ICN", "GMP", "PEK", "PVG", "HKG", "TPE", "BKK",
    "SIN", "KUL", "MNL", "CGK", "DEL", "BOM", "MAA", "BLR", "CCU", "SYD",
    "MEL", "BNE", "AKL", "WLG", "CHC", "NAN", "PPT",
    # Middle East & Africa
    "DXB", "AUH", "DOH", "RUH", "JED", "TLV", "AMM", "CAI", "CMN", "JNB",
    "CPT", "NBO", "ADD", "LOS", "ACC", "DAR",
    # Hawaii (extra)
    "ITO",
    # Pacific NW regional
    "BLI", "RDM", "MFR", "EAT", "PSC", "ALW", "PUW", "LWS",
}

# Non-US IATA codes (for international flag detection)
# This is a simplified check: US codes are 3 letters and we maintain a US set
US_IATA_CODES = {
    "ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "SEA", "LAS", "MCO",
    "CLT", "EWR", "PHX", "IAH", "MIA", "BOS", "MSP", "FLL", "DTW", "PHL",
    "LGA", "BWI", "SLC", "SAN", "IAD", "DCA", "MDW", "TPA", "PDX", "HNL",
    "STL", "BNA", "AUS", "HOU", "OAK", "MSY", "RDU", "SJC", "SMF", "SNA",
    "MCI", "SAT", "PIT", "CLE", "CMH", "IND", "CVG", "BDL", "JAX", "ABQ",
    "ANC", "SDF", "RNO", "OMA", "MEM", "BUF", "OGG", "PBI", "TUS", "OKC",
    "ELP", "ONT", "BUR", "RSW", "RIC", "ORF", "BOI", "LIT", "GEG", "DSM",
    "KOA", "LIH", "ICT", "CHS", "DAY", "TUL", "GRR", "PVD", "MKE", "BHM",
    "SYR", "SAV", "PSP", "GSP", "HSV", "FAT", "COS", "ISP", "ROC", "ALB",
    "SRQ", "TYS", "XNA", "PNS", "DAB", "MSN", "LEX", "PWM", "MYR", "CAK",
    "BTR", "GSO", "FAI", "BZN", "FSD", "RAP", "AZO", "MOB", "SGF", "CRW",
    "AVL", "JNU", "GNV", "EUG", "MLB", "LBB", "AMA", "FNT", "SBN", "GRB",
    "BTV", "TLH", "CID", "LAN", "MFE", "SHV", "EVV", "FWA", "GPT", "MHT",
    "MLI", "TRI", "VPS", "PIH", "BIL", "JAC", "MTJ", "HDN", "EGE", "SUN",
    "STT", "STX", "SJU", "AKN", "ADQ", "BET", "CDV", "DLG", "SIT", "YAK",
    "KTN", "WRG", "PSG", "OME", "OTZ", "BRW", "ITO", "BLI", "RDM", "MFR",
    "EAT", "PSC", "ALW", "PUW", "LWS",
}

# Common 3-letter words that are NOT IATA codes (false positive filter)
NON_IATA_WORDS = {
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "ANY", "CAN",
    "HER", "WAS", "ONE", "OUR", "OUT", "DAY", "GET", "HAS", "HIM", "HIS",
    "HOW", "ITS", "LET", "MAY", "NEW", "NOW", "OLD", "SEE", "WAY", "WHO",
    "DID", "GOT", "HAS", "HER", "LET", "SAY", "SHE", "TOO", "USE", "DAD",
    "MOM", "SIS", "BRO", "PRE", "PER", "SET", "RUN", "PUT", "ADD", "END",
    "FEW", "TOP", "RED", "SUN", "FUN", "YES", "YET", "BIG", "TRY", "ASK",
    "MEN", "RAN", "LOW", "OWN", "SAW", "BED", "JOB", "CUT", "AGE", "AGO",
    "AIR", "BAD", "BAG", "BAR", "BIT", "BOX", "BOY", "BUS", "CAR", "CUP",
    "DOG", "EAR", "EAT", "EYE", "FAR", "FIT", "GAS", "HAD", "HAT", "HOT",
    "ICE", "ILL", "KEY", "LAW", "LAY", "LEG", "LIE", "LOT", "MAP", "MIX",
    "NET", "NOR", "OIL", "PAY", "PEN", "PIN", "POT", "RAW", "ROW", "SIT",
    "SIX", "SKI", "TAX", "TEN", "TIE", "TIP", "TWO", "VIA", "WAR", "WET",
    "WIN", "WON", "WOO", "ZIP",
    # Tech/email terms
    "CSS", "XML", "SQL", "API", "URL", "FAQ", "PDF", "RSS", "FTP", "SSH",
    "DNS", "IDE", "SDK", "APP", "LOG", "DEV", "OPS", "SRC", "BIN", "DOC",
    "ERR", "MSG", "TXT", "PNG", "JPG", "GIF", "SVG", "CSV", "TSV",
    # Business/email
    "CEO", "CFO", "CTO", "COO", "SVP", "EVP", "MGR", "DIR", "REP", "ADV",
    "REF", "FYI", "EOD", "EOW", "TBD", "TBA", "ETA",
}


def progress(msg: str) -> None:
    """Print timestamped progress for SSM monitoring."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTML -> plain text conversion (stdlib only)
# ---------------------------------------------------------------------------

class HTMLTextExtractor(HTMLParser):
    """Minimal HTML to text converter using stdlib html.parser."""

    def __init__(self):
        super().__init__()
        self._text_parts: List[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4"):
            self._text_parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._text_parts.append(data)

    def get_text(self) -> str:
        return " ".join("".join(self._text_parts).split())

    def error(self, message):
        pass  # suppress parse errors


def html_to_text(html: str) -> str:
    """Convert HTML to plain text."""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html)
        return extractor.get_text()
    except Exception:
        # Fallback: regex strip
        text = re.sub(r'<[^>]+>', ' ', html)
        return " ".join(text.split())


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def download_from_s3(s3, bucket: str, key: str, local_path: str) -> None:
    """Download an S3 object to local path."""
    progress(f"  Downloading s3://{bucket}/{key}")
    s3.download_file(bucket, key, local_path)
    size = os.path.getsize(local_path)
    progress(f"  Download complete ({size:,} bytes)")


def upload_to_s3(s3, local_path: str, bucket: str, key: str, content_type: str = None) -> None:
    """Upload a local file to S3."""
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    progress(f"  Uploading to s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key, ExtraArgs=extra if extra else None)
    progress(f"  Upload complete")


# ---------------------------------------------------------------------------
# Email body extraction
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


def extract_body(msg) -> str:
    """
    Extract text body from an email message.
    Prefers text/plain, falls back to text/html converted to text.
    Handles multipart MIME by walking all parts.
    """
    plain_parts = []
    html_parts = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            # Skip attachments
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition.lower():
                continue

            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    plain_parts.append(payload.decode(charset, errors="replace"))
            elif content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html_parts.append(payload.decode(charset, errors="replace"))
    else:
        content_type = msg.get_content_type()
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if content_type == "text/plain":
                plain_parts.append(text)
            elif content_type == "text/html":
                html_parts.append(text)

    # Prefer plain text
    if plain_parts:
        return "\n".join(plain_parts)

    # Fall back to HTML -> text
    if html_parts:
        return "\n".join(html_to_text(h) for h in html_parts)

    # Last resort: try get_payload as string
    try:
        payload = msg.get_payload()
        if isinstance(payload, str):
            return payload
    except Exception:
        pass

    return ""


# ---------------------------------------------------------------------------
# Flight signal detection & classification
# ---------------------------------------------------------------------------

def classify_email(from_hdr: str, subject: str, domain: str) -> Optional[int]:
    """
    Classify email into extraction tier.
    Returns 1 (airline), 2 (OTA/travel service), or None (skip).
    """
    # Tier 1: Direct airline sender
    if domain in AIRLINE_DOMAINS:
        return 1

    # Check subject for Tier 1 signal with airline domain partial match
    for airline_domain in AIRLINE_DOMAINS:
        if airline_domain.split(".")[0] in domain:
            return 1

    # Tier 1: Subject contains strong booking signals
    lower_subject = subject.lower()
    has_booking_subject = any(p in lower_subject for p in TIER1_SUBJECT_PATTERNS)

    # Tier 2: OTA/travel service sender
    if domain in OTA_DOMAINS:
        # Special handling for expedia.com: only extract if email has booking structure
        if "expedia" in domain:
            if has_booking_subject:
                return 2
            # Skip work-related Expedia emails
            return None
        return 2

    # Check OTA partial domain match
    for ota_domain in OTA_DOMAINS:
        if ota_domain.split(".")[0] in domain and ota_domain.split(".")[0] != "google":
            if "expedia" in domain:
                if has_booking_subject:
                    return 2
                return None
            return 2

    # Tier 1 via subject if sender is unknown but subject is strong
    if has_booking_subject:
        # Also check if subject mentions an airline name
        airline_names = [
            "alaska", "american", "delta", "united", "southwest",
            "jetblue", "spirit", "frontier", "hawaiian",
        ]
        if any(name in lower_subject for name in airline_names):
            return 1

    return None


def is_expedia_work_email(body: str, subject: str) -> bool:
    """
    Heuristic: detect if an Expedia-domain email is work-related rather
    than a personal booking.
    """
    lower_body = body.lower()
    lower_subject = subject.lower()

    # Work signals
    work_signals = [
        "sprint", "standup", "jira", "confluence", "pull request",
        "code review", "deploy", "meeting", "calendar invite",
        "org chart", "team", "1:1", "all-hands", "onboarding",
        "offboarding", "performance review", "OKR", "roadmap",
    ]
    work_score = sum(1 for s in work_signals if s in lower_body or s in lower_subject)

    # Booking signals
    booking_signals = [
        "confirmation", "itinerary", "e-ticket", "booking",
        "depart", "arrival", "passenger", "seat", "boarding",
        "flight number", "airport",
    ]
    booking_score = sum(1 for s in booking_signals if s in lower_body)

    return work_score > booking_score


# ---------------------------------------------------------------------------
# Field extraction from email body
# ---------------------------------------------------------------------------

def extract_confirmation_code(body: str) -> Optional[str]:
    """Extract a 6-character confirmation/PNR code from email body."""
    # Look for labeled confirmation codes first
    labeled_patterns = [
        re.compile(
            r'(?:confirmation|booking|record\s+locator|PNR|reservation)\s*'
            r'(?:code|number|#|:)?\s*[:# ]?\s*([A-Z][A-Z0-9]{5})\b',
            re.IGNORECASE
        ),
    ]
    for pattern in labeled_patterns:
        match = pattern.search(body)
        if match:
            code = match.group(1).upper()
            # Filter out common false positives
            if code not in NON_IATA_WORDS and not code.isdigit():
                return code

    # Fall back to unlabeled 6-char codes near flight context
    context_re = re.compile(
        r'(?:confirm|book|reserv|itinerary|locator).{0,40}?([A-Z][A-Z0-9]{5})\b',
        re.IGNORECASE
    )
    match = context_re.search(body)
    if match:
        code = match.group(1).upper()
        if code not in NON_IATA_WORDS and not code.isdigit():
            return code

    return None


def extract_airports(body: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract origin and destination IATA airport codes from email body.
    Returns (origin, dest) tuple.
    """
    # Strategy 1: Look for route patterns like "SEA-SFO" or "SEA to SFO"
    route_matches = ROUTE_RE.findall(body)
    for orig, dest in route_matches:
        if (orig in IATA_CODES and orig not in NON_IATA_WORDS and
                dest in IATA_CODES and dest not in NON_IATA_WORDS):
            return orig, dest

    # Strategy 2: Look for airport codes in departure/arrival context
    context_matches = AIRPORT_CONTEXT_RE.findall(body)
    valid_codes = [c.upper() for c in context_matches
                   if c.upper() in IATA_CODES and c.upper() not in NON_IATA_WORDS]
    if len(valid_codes) >= 2:
        return valid_codes[0], valid_codes[1]

    # Strategy 3: Find all standalone 3-letter codes that are valid IATA
    all_codes = IATA_STANDALONE_RE.findall(body)
    valid_standalone = []
    seen = set()
    for code in all_codes:
        if code in IATA_CODES and code not in NON_IATA_WORDS and code not in seen:
            valid_standalone.append(code)
            seen.add(code)

    if len(valid_standalone) >= 2:
        return valid_standalone[0], valid_standalone[1]
    elif len(valid_standalone) == 1:
        return valid_standalone[0], None

    return None, None


def extract_flight_number(body: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract flight number and infer airline from it.
    Returns (flight_number_str, airline_name).
    """
    matches = FLIGHT_NUMBER_RE.findall(body)
    if matches:
        prefix, number = matches[0]
        flight_num = f"{prefix}{number}"
        airline = FLIGHT_NUMBER_AIRLINES.get(prefix)
        return flight_num, airline
    return None, None


def extract_fares(body: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract fare amounts from email body.
    Returns (base_fare, total_paid) -- best guess.
    """
    matches = FARE_RE.findall(body)
    if not matches:
        return None, None

    amounts = []
    for m in matches:
        try:
            val = float(m.replace(",", ""))
            if 10.0 <= val <= 50000.0:  # reasonable fare range
                amounts.append(val)
        except ValueError:
            continue

    if not amounts:
        return None, None

    # Heuristic: if multiple amounts, largest is likely total_paid
    amounts.sort()
    if len(amounts) == 1:
        return None, amounts[0]
    else:
        # Smallest reasonable amount as base, largest as total
        return amounts[0], amounts[-1]


def extract_flight_dates(body: str, email_date: Optional[datetime]) -> List[datetime]:
    """
    Extract flight dates from email body.
    Returns list of datetime objects for detected flight dates.
    """
    dates = []

    for pattern, fmt_hint in DATE_PATTERNS:
        for match in pattern.finditer(body):
            try:
                groups = match.groups()
                if fmt_hint == "%B %d %Y":
                    month_str, day_str, year_str = groups
                    month = MONTH_MAP.get(month_str.lower())
                    if month:
                        dt = datetime(int(year_str), month, int(day_str), tzinfo=timezone.utc)
                        dates.append(dt)
                elif fmt_hint == "MDY":
                    m, d, y = groups
                    dt = datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
                    dates.append(dt)
                elif fmt_hint == "ISO":
                    y, m, d = groups
                    dt = datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
                    dates.append(dt)
                elif fmt_hint == "DMY":
                    d, month_str, y = groups
                    month = MONTH_MAP.get(month_str.lower())
                    if month:
                        dt = datetime(int(y), month, int(d), tzinfo=timezone.utc)
                        dates.append(dt)
            except (ValueError, TypeError):
                continue

    # Filter to reasonable date range (2000-2030) and deduplicate
    valid_dates = []
    seen = set()
    for dt in dates:
        if 2000 <= dt.year <= 2030:
            key = dt.strftime("%Y-%m-%d")
            if key not in seen:
                valid_dates.append(dt)
                seen.add(key)

    # Sort: prefer dates in the future relative to email date (flight dates
    # are usually after the email was sent)
    if email_date and valid_dates:
        # Separate future and past dates
        future = [d for d in valid_dates if d >= email_date.replace(hour=0, minute=0, second=0)]
        past = [d for d in valid_dates if d < email_date.replace(hour=0, minute=0, second=0)]
        valid_dates = sorted(future) + sorted(past)

    return valid_dates


def identify_airline(domain: str, body: str, flight_num_airline: Optional[str]) -> Optional[str]:
    """Identify airline from domain, body content, or flight number."""
    # From domain
    if domain in AIRLINE_DOMAINS:
        return AIRLINE_DOMAINS[domain]

    # From flight number
    if flight_num_airline:
        return flight_num_airline

    # From body text
    airline_body_patterns = {
        "Alaska Airlines": ["alaska airlines", "alaskaair"],
        "American Airlines": ["american airlines"],
        "Delta Air Lines": ["delta air lines", "delta airlines"],
        "United Airlines": ["united airlines"],
        "Southwest Airlines": ["southwest airlines"],
        "JetBlue Airways": ["jetblue"],
        "Spirit Airlines": ["spirit airlines"],
        "Frontier Airlines": ["frontier airlines"],
        "Hawaiian Airlines": ["hawaiian airlines"],
    }
    lower_body = body.lower()
    for airline, patterns in airline_body_patterns.items():
        if any(p in lower_body for p in patterns):
            return airline

    return None


def compute_confidence(record: dict) -> float:
    """Compute extraction confidence score (0.0 - 1.0)."""
    score = 0.0

    if record.get("confirmation_code"):
        score += 0.3
    if record.get("origin") and record.get("dest"):
        score += 0.2
    elif record.get("origin") or record.get("dest"):
        score += 0.1
    if record.get("airline"):
        score += 0.2
    if record.get("flight_number"):
        score += 0.15
    if record.get("total_paid") or record.get("base_fare"):
        score += 0.1
    if record.get("_date_from_body"):
        score += 0.05

    return min(score, 1.0)


def generate_flight_id(origin: str, dest: str, date_iso: str,
                       confirmation_code: Optional[str]) -> str:
    """Deterministic flight_id via SHA256."""
    origin = origin or "UNK"
    dest = dest or "UNK"
    conf = confirmation_code or "NONE"
    raw = f"{origin}#{dest}#{date_iso}#{conf}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def is_international(origin: Optional[str], dest: Optional[str]) -> bool:
    """Check if either airport is non-US."""
    if origin and origin not in US_IATA_CODES:
        return True
    if dest and dest not in US_IATA_CODES:
        return True
    return False


# ---------------------------------------------------------------------------
# Per-email extraction pipeline
# ---------------------------------------------------------------------------

def extract_from_email(msg, source_file: str) -> List[dict]:
    """
    Extract flight record(s) from a single email message.
    Returns list of flight record dicts (may be 0 or more).
    """
    from_hdr = _safe_header(msg, "From")
    subject = _safe_header(msg, "Subject")
    date_hdr = _safe_header(msg, "Date")
    domain = _extract_domain(from_hdr)

    # Classify tier
    tier = classify_email(from_hdr, subject, domain)
    if tier is None:
        return []

    # Extract body
    body = extract_body(msg)
    if not body or len(body) < 20:
        return []

    # For Expedia domain, additional work-email filter
    if "expedia" in domain:
        if is_expedia_work_email(body, subject):
            return []

    # Parse email date
    email_date = None
    if date_hdr:
        try:
            email_date = parsedate_to_datetime(date_hdr)
        except Exception:
            pass

    # Extract fields
    confirmation_code = extract_confirmation_code(body)
    origin, dest = extract_airports(body)
    flight_number, flight_num_airline = extract_flight_number(body)
    base_fare, total_paid = extract_fares(body)
    airline = identify_airline(domain, body, flight_num_airline)
    flight_dates = extract_flight_dates(body, email_date)

    # Determine flight date: prefer body-extracted date, fall back to email date
    date_from_body = False
    if flight_dates:
        flight_dt = flight_dates[0]
        date_from_body = True
    elif email_date:
        flight_dt = email_date
    else:
        # No date at all -- still record with empty date
        flight_dt = None

    # Must have at least SOME flight signal to be worth recording
    has_signal = (confirmation_code or origin or dest or
                  flight_number or airline or
                  (total_paid and total_paid > 50))
    if not has_signal:
        return []

    # Build record
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if flight_dt:
        date_iso = flight_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_yyyy_mm = flight_dt.strftime("%Y-%m")
        source_year = str(flight_dt.year) if date_from_body else (
            str(email_date.year) if email_date else ""
        )
    else:
        date_iso = ""
        date_yyyy_mm = ""
        source_year = ""

    record = {
        "flight_id": generate_flight_id(origin, dest, date_iso, confirmation_code),
        "date_iso": date_iso,
        "date_yyyy_mm": date_yyyy_mm,
        "origin": origin or "",
        "dest": dest or "",
        "airline": airline or "",
        "booked_class": "",  # not reliably extractable from emails
        "confirmation_code": confirmation_code or "",
        "flight_number": flight_number or "",
        "base_fare": base_fare if base_fare else None,
        "total_paid": total_paid if total_paid else None,
        "trip_city": dest or "",  # best guess: destination is the trip city
        "cost_type": "",
        "international": is_international(origin, dest),
        "eqm_miles": None,
        "status": "candidate",  # not yet validated
        "source_year": source_year,
        "source_sheet": source_file.replace(".mbox", ""),
        "website": domain,
        "funding_source": "",
        "funding_date": "",
        "sequence": "",
        "created_at": now_iso,
        "updated_at": now_iso,
        # Metadata columns (not in DynamoDB schema but useful for review)
        "extraction_confidence": 0.0,  # computed below
        "extraction_tier": tier,
        "source_file": source_file,
        "source_email_subject": subject[:300],
        "source_email_date": email_date.isoformat() if email_date else "",
        "_date_from_body": date_from_body,
    }

    record["extraction_confidence"] = compute_confidence(record)

    # For OTA emails with multiple flight segments, attempt to find additional
    # route pairs. Only if we found more than 2 IATA codes.
    records = [record]

    if tier == 2 and flight_dates and len(flight_dates) > 1:
        # Try extracting a return flight from additional dates
        # Only if we have both origin and dest (for a round trip)
        if origin and dest and len(flight_dates) >= 2:
            return_dt = flight_dates[1]
            return_date_iso = return_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            return_record = dict(record)
            return_record["flight_id"] = generate_flight_id(
                dest, origin, return_date_iso, confirmation_code
            )
            return_record["date_iso"] = return_date_iso
            return_record["date_yyyy_mm"] = return_dt.strftime("%Y-%m")
            return_record["origin"] = dest
            return_record["dest"] = origin
            return_record["trip_city"] = origin  # returning to origin
            return_record["source_year"] = str(return_dt.year)
            return_record["_date_from_body"] = True
            return_record["extraction_confidence"] = compute_confidence(return_record)
            records.append(return_record)

    # Drop the internal _date_from_body flag
    for r in records:
        del r["_date_from_body"]

    return records


# ---------------------------------------------------------------------------
# MBOX processing
# ---------------------------------------------------------------------------

def _should_extract_body(msg, source_file: str) -> bool:
    """
    Quick header-only pre-filter: decide if this email is worth parsing
    the body for. Avoids loading large bodies for non-flight emails,
    which is critical for memory on constrained instances.
    """
    from_hdr = _safe_header(msg, "From")
    subject = _safe_header(msg, "Subject")
    domain = _extract_domain(from_hdr)
    tier = classify_email(from_hdr, subject, domain)
    return tier is not None


def _report_memory() -> str:
    """Return current RSS in MB."""
    try:
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On Linux ru_maxrss is in KB; on macOS it's bytes
        if sys.platform == "linux":
            rss_mb = rss_kb / 1024
        else:
            rss_mb = rss_kb / (1024 * 1024)
        return f"{rss_mb:.0f}MB RSS"
    except Exception:
        return "unknown RSS"


def process_mbox_file(s3, bucket: str, key: str, tmpdir: str) -> Tuple[List[dict], dict]:
    """
    Download and process a single MBOX file.
    Returns (records, stats_dict).

    Memory optimization: pre-filters on headers before body extraction,
    runs GC every 500 messages to keep memory bounded.
    """
    source_file = os.path.basename(key)
    local_path = os.path.join(tmpdir, source_file)

    download_from_s3(s3, bucket, key, local_path)

    progress(f"  Opening MBOX: {source_file} ({_report_memory()})")
    mbox = mailbox.mbox(local_path)

    total = 0
    signal_count = 0
    records = []
    errors = 0
    skipped_header_filter = 0

    for msg in mbox:
        total += 1
        if total % 1000 == 0:
            progress(f"    ...processed {total:,} emails, {len(records)} records, "
                     f"{skipped_header_filter} header-filtered ({source_file}) [{_report_memory()}]")

        # Memory management: force GC periodically
        if total % 500 == 0:
            gc.collect()

        try:
            # Pre-filter on headers only (cheap) before body extraction (expensive)
            if not _should_extract_body(msg, source_file):
                skipped_header_filter += 1
                continue

            extracted = extract_from_email(msg, source_file)
            if extracted:
                signal_count += 1
                records.extend(extracted)
        except Exception as e:
            errors += 1
            if errors <= 10:
                progress(f"    WARN: Error processing email #{total}: {type(e).__name__}: {e}")

    mbox.close()
    del mbox
    gc.collect()

    # Clean up downloaded file to free disk
    try:
        os.remove(local_path)
        progress(f"  Cleaned up {local_path}")
    except OSError:
        pass

    gc.collect()

    stats = {
        "source_file": source_file,
        "total_emails": total,
        "signal_emails": signal_count,
        "records_extracted": len(records),
        "errors": errors,
        "header_filtered": skipped_header_filter,
    }

    progress(f"  MBOX complete: {source_file} -- {total:,} emails, "
             f"{signal_count:,} with signal, {len(records)} records, "
             f"{skipped_header_filter} header-filtered, {errors} errors [{_report_memory()}]")

    return records, stats


# ---------------------------------------------------------------------------
# Parquet output
# ---------------------------------------------------------------------------

def records_to_parquet(records: List[dict], output_path: str) -> None:
    """Write extracted records to Parquet file."""
    if not records:
        progress("  WARNING: No records to write to Parquet")
        return

    # Define schema
    schema = pa.schema([
        ("flight_id", pa.string()),
        ("date_iso", pa.string()),
        ("date_yyyy_mm", pa.string()),
        ("origin", pa.string()),
        ("dest", pa.string()),
        ("airline", pa.string()),
        ("booked_class", pa.string()),
        ("confirmation_code", pa.string()),
        ("flight_number", pa.string()),
        ("base_fare", pa.float64()),
        ("total_paid", pa.float64()),
        ("trip_city", pa.string()),
        ("cost_type", pa.string()),
        ("international", pa.bool_()),
        ("eqm_miles", pa.float64()),
        ("status", pa.string()),
        ("source_year", pa.string()),
        ("source_sheet", pa.string()),
        ("website", pa.string()),
        ("funding_source", pa.string()),
        ("funding_date", pa.string()),
        ("sequence", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
        # Metadata
        ("extraction_confidence", pa.float64()),
        ("extraction_tier", pa.int64()),
        ("source_file", pa.string()),
        ("source_email_subject", pa.string()),
        ("source_email_date", pa.string()),
    ])

    # Build column arrays
    columns = {field.name: [] for field in schema}

    for rec in records:
        for field in schema:
            val = rec.get(field.name)
            columns[field.name].append(val)

    arrays = []
    for field in schema:
        col_data = columns[field.name]
        arrays.append(pa.array(col_data, type=field.type))

    table = pa.table(arrays, schema=schema)
    pq.write_table(table, output_path, compression="snappy")

    progress(f"  Parquet written: {output_path} ({os.path.getsize(output_path):,} bytes, "
             f"{len(records)} rows)")


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def build_summary(all_records: List[dict], file_stats: List[dict]) -> dict:
    """Build execution summary JSON."""
    total_emails = sum(s["total_emails"] for s in file_stats)
    signal_emails = sum(s["signal_emails"] for s in file_stats)
    total_errors = sum(s["errors"] for s in file_stats)

    # Confidence distribution
    high = sum(1 for r in all_records if r["extraction_confidence"] >= 0.7)
    medium = sum(1 for r in all_records if 0.4 <= r["extraction_confidence"] < 0.7)
    low = sum(1 for r in all_records if r["extraction_confidence"] < 0.4)

    # Field completeness
    fields_to_check = [
        "confirmation_code", "origin", "dest", "airline", "flight_number",
        "base_fare", "total_paid", "date_iso",
    ]
    completeness = {}
    for field in fields_to_check:
        filled = sum(1 for r in all_records
                     if r.get(field) and r[field] != "" and r[field] is not None)
        pct = f"{100 * filled / len(all_records):.1f}%" if all_records else "0%"
        completeness[field] = pct

    # By source file
    by_source = defaultdict(int)
    for r in all_records:
        by_source[r.get("source_file", "unknown")] += 1

    # By airline
    by_airline = defaultdict(int)
    for r in all_records:
        airline = r.get("airline", "")
        if airline:
            by_airline[airline] += 1
        else:
            by_airline["(unknown)"] += 1

    # By year
    by_year = defaultdict(int)
    for r in all_records:
        yr = r.get("source_year", "")
        if yr:
            by_year[yr] += 1

    # International count
    intl_count = sum(1 for r in all_records if r.get("international"))

    # Unique flight_ids (dedup check)
    unique_ids = len(set(r["flight_id"] for r in all_records))
    duplicate_ids = len(all_records) - unique_ids

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_emails_scanned": total_emails,
        "flight_signal_emails": signal_emails,
        "records_extracted": len(all_records),
        "unique_flight_ids": unique_ids,
        "duplicate_flight_ids": duplicate_ids,
        "total_extraction_errors": total_errors,
        "confidence_distribution": {
            "high_0.7+": high,
            "medium_0.4_0.7": medium,
            "low_0_0.4": low,
        },
        "field_completeness": completeness,
        "by_source_file": dict(sorted(by_source.items())),
        "by_airline": dict(sorted(by_airline.items(), key=lambda x: x[1], reverse=True)),
        "by_year": dict(sorted(by_year.items())),
        "international_flights": intl_count,
        "file_processing_stats": file_stats,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Flight extraction pipeline for jreesegpt corpus (Phase 2)"
    )
    parser.add_argument("--source-bucket", default="jreesegpt-private",
                        help="S3 bucket with MBOX corpus (default: jreesegpt-private)")
    parser.add_argument("--source-prefix", default="corpus-raw/2025-09-14/",
                        help="S3 prefix for MBOX files")
    parser.add_argument("--output-bucket", default="devops-agentcli-compute",
                        help="Output S3 bucket")
    parser.add_argument("--output-prefix",
                        default="projects/travel/flight-candidates/raw/",
                        help="Output S3 prefix")
    parser.add_argument("--region", default="us-west-2",
                        help="AWS region (default: us-west-2)")
    parser.add_argument("--tmpdir", default=None,
                        help="Explicit temp directory (default: auto-detect, "
                             "prefers /home/ec2-user over /tmp for large files)")
    args = parser.parse_args()

    progress("=" * 70)
    progress("Flight Extraction Pipeline -- Phase 2")
    progress(f"Source: s3://{args.source_bucket}/{args.source_prefix}")
    progress(f"Output: s3://{args.output_bucket}/{args.output_prefix}")
    progress("=" * 70)

    s3 = boto3.client("s3", region_name=args.region)

    all_records: List[dict] = []
    file_stats: List[dict] = []

    # Determine temp directory: prefer a non-tmpfs location for large MBOX files
    # EC2 instances often have small tmpfs on /tmp but more space on /home
    if args.tmpdir:
        tmpdir_base = args.tmpdir
    elif os.path.isdir("/home/ec2-user"):
        tmpdir_base = "/home/ec2-user/extract_tmp"
    else:
        tmpdir_base = None  # use system default

    if tmpdir_base:
        os.makedirs(tmpdir_base, exist_ok=True)
        tmpdir_ctx = tempfile.TemporaryDirectory(prefix="flight_extract_", dir=tmpdir_base)
    else:
        tmpdir_ctx = tempfile.TemporaryDirectory(prefix="flight_extract_")

    with tmpdir_ctx as tmpdir:
        progress(f"Temp directory: {tmpdir}")

        # Report available disk space
        try:
            stat = os.statvfs(tmpdir)
            avail_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
            progress(f"  Available disk space: {avail_gb:.1f} GB")
        except Exception:
            pass

        for mbox_name in MBOX_FILES:
            s3_key = f"{args.source_prefix}{mbox_name}"
            progress(f"\nProcessing: {mbox_name}")

            # Check if file exists
            try:
                s3.head_object(Bucket=args.source_bucket, Key=s3_key)
            except Exception as e:
                progress(f"  WARN: Could not access s3://{args.source_bucket}/{s3_key}: {e}")
                progress(f"  Skipping {mbox_name}")
                continue

            records, stats = process_mbox_file(
                s3, args.source_bucket, s3_key, tmpdir
            )
            all_records.extend(records)
            file_stats.append(stats)

        # Deduplicate by flight_id (keep highest confidence)
        progress(f"\nTotal records before dedup: {len(all_records)}")
        dedup_map: Dict[str, dict] = {}
        for rec in all_records:
            fid = rec["flight_id"]
            if fid not in dedup_map or rec["extraction_confidence"] > dedup_map[fid]["extraction_confidence"]:
                dedup_map[fid] = rec
        all_records = list(dedup_map.values())
        progress(f"Total records after dedup: {len(all_records)}")

        # Write Parquet
        parquet_path = os.path.join(tmpdir, "flight-candidates-raw.parquet")
        progress("\nWriting Parquet output...")
        records_to_parquet(all_records, parquet_path)

        # Build and write summary
        progress("Building execution summary...")
        summary = build_summary(all_records, file_stats)
        summary_path = os.path.join(tmpdir, "extraction-summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        # Upload to S3
        progress("\nUploading results to S3...")
        if all_records:
            upload_to_s3(
                s3, parquet_path, args.output_bucket,
                f"{args.output_prefix}flight-candidates-raw.parquet",
                content_type="application/octet-stream"
            )
        upload_to_s3(
            s3, summary_path, args.output_bucket,
            f"{args.output_prefix}extraction-summary.json",
            content_type="application/json"
        )

    # Print summary to stdout for SSM monitoring
    progress("\n" + "=" * 70)
    progress("EXTRACTION COMPLETE")
    progress(f"  Emails scanned:     {summary['total_emails_scanned']:,}")
    progress(f"  Signal emails:      {summary['flight_signal_emails']:,}")
    progress(f"  Records extracted:  {summary['records_extracted']:,}")
    progress(f"  Unique flight IDs:  {summary['unique_flight_ids']:,}")
    progress(f"  Duplicates removed: {summary['duplicate_flight_ids']:,}")
    progress(f"  Confidence: high={summary['confidence_distribution']['high_0.7+']} "
             f"med={summary['confidence_distribution']['medium_0.4_0.7']} "
             f"low={summary['confidence_distribution']['low_0_0.4']}")
    progress(f"  Field completeness:")
    for field, pct in summary["field_completeness"].items():
        progress(f"    {field}: {pct}")
    progress(f"  By airline:")
    for airline, count in summary["by_airline"].items():
        progress(f"    {airline}: {count}")
    progress(f"  International flights: {summary['international_flights']}")
    progress(f"  Parquet: s3://{args.output_bucket}/{args.output_prefix}flight-candidates-raw.parquet")
    progress(f"  Summary: s3://{args.output_bucket}/{args.output_prefix}extraction-summary.json")
    progress("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
