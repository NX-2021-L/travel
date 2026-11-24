"""Read-only @io-kit/records routes (FLY-TSK-083, FLY-FTR-004): travel proves the records contract is general.

  GET /records/recordsets
  GET /records/recordsets/{id}/descriptor
  GET /records/recordsets/{id}/rows       [?today=YYYY-MM-DD&near_home_km=N for trips] [?limit=N&cursor=...]

Two sets, both served from what the tools already read; there is no business logic here, only shape:
  trips             class derived, order fixed. Rows are lane_3a_trips._compute (trip_engine.compute_trips) ordered by
                    trip_engine.order_trips_for_listing. Params are the engine's set-level parameters (today, near_home_km).
                    Each row's source lists its record ids; excluded records, unapplied corrections and duplicate inputs
                    are findings.
  trip_corrections  class amend_with_audit, NO capabilities (travel writes corrections through its tools, not here):
                    lane_2e_corrections.load_corrections, retired ones included.

Auth: a human-door token only (records_door.authenticate). Every refusal, descriptors included, is the same 401 problem.
The routes are not tools: they never appear in tools/list, caps.json or the permission manifest (ui_only).
Responses carry no ids or values of any deployment beyond the rows the caller is entitled to read.
"""
import asyncio
import base64
import hashlib
import inspect
import json
import re
import sys
import urllib.parse
from datetime import date

import lane_2e_corrections
import records_door
import trip_engine

PREFIX = "/records"
JSON = "application/json"
PROBLEM = "application/problem+json"

SOURCE_SET = "travel_records"  # lineage target: the underlying travel records, which travel does not (yet) serve as a set
TRIPS, CORRECTIONS = "trips", "trip_corrections"


def _col(id_, label, type_, **hint):
    c = {"id": id_, "label": label, "type": type_, "editable": False}
    if hint:
        c["hint"] = hint
    return c


TRIP_COLUMNS = [
    _col("key", "Trip", "text"),
    _col("first_date", "First date", "date"), _col("first_basis", "First date basis", "enum"),
    _col("last_date", "Last date", "date"), _col("last_basis", "Last date basis", "enum"),
    _col("nights", "Nights", "number", align="end"), _col("nights_exact", "Nights exact", "boolean"),
    _col("state", "State", "enum"),
    _col("places_visited", "Places visited", "json", default_hidden=True),
    _col("flags", "Flags", "json"), _col("signals", "Signals", "json"),
    _col("legs", "Legs", "json", default_hidden=True), _col("stays", "Stays", "json", default_hidden=True),
    _col("rentals", "Rentals", "json", default_hidden=True),
    _col("corrections", "Corrections", "json", default_hidden=True),
    _col("record_ids", "Record ids", "json", default_hidden=True),
]
CORRECTION_COLUMNS = [
    _col("correction_id", "Correction", "text"), _col("kind", "Kind", "enum"), _col("status", "Status", "enum"),
    _col("date", "Date", "text"), _col("date_iso", "Date (ISO)", "date"),
    _col("record_ids", "Record ids", "json"), _col("reason", "Reason", "text"),
    _col("author", "Author", "text"), _col("session", "Session", "text"), _col("question_id", "Question", "text"),
    _col("created_at", "Created", "text"), _col("retired_at", "Retired", "text"), _col("retired_by", "Retired by", "text"),
    _col("first_date", "First date", "date"), _col("last_date", "Last date", "date"),
    _col("method", "Method", "enum"), _col("supersedes", "Supersedes", "json"),
]
_NEAR_HOME_DEFAULT = inspect.signature(trip_engine.compute_trips).parameters["near_home_km"].default


def _lane():
    mod = sys.modules.get("lane_3a_trips")
    if mod is None:
        import lane_3a_trips as mod  # noqa: F401
    return mod


def descriptors() -> dict:
    today = _lane()._today().isoformat()
    return {
        TRIPS: {"id": TRIPS, "title": "Trips", "key": "key", "class": "derived", "capabilities": [], "reason": "none",
                "collection": "client", "order": "fixed", "inputs": [CORRECTIONS],
                "params": [{"id": "today", "type": "date", "options_from": None, "default": today},
                           {"id": "near_home_km", "type": "number", "options_from": None, "default": _NEAR_HOME_DEFAULT}],
                "columns": TRIP_COLUMNS},
        CORRECTIONS: {"id": CORRECTIONS, "title": "Trip corrections", "key": "correction_id", "class": "amend_with_audit",
                      "capabilities": [], "reason": "none", "collection": "client", "order": "free",
                      "columns": CORRECTION_COLUMNS},
    }


# --------------------------------------------------------------------------- responses
def _resp(status, body, ctype=JSON, extra=None):
    headers = {"content-type": ctype, "cache-control": "no-store"}
    headers.update(extra or {})
    return {"statusCode": status, "headers": headers, "body": json.dumps(body), "isBase64Encoded": False}


def problem(status, title, detail=None, field=None, extra=None):
    body = {"type": f"about:blank#{title.lower().replace(' ', '-')}", "title": title, "status": status}
    if detail:
        body["detail"] = detail
    if field is not None:
        body["field"] = field
    return _resp(status, body, PROBLEM, extra)


def unauthorized():
    """One answer for every refusal (no token, an API key, a connector bearer, a wrong issuer or audience, expired)."""
    return problem(401, "Unauthorized", "A human sign-in token is required.", extra={"www-authenticate": "Bearer"})


# --------------------------------------------------------------------------- rows
def _vintage(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _version(values) -> str:
    return _vintage(values)


def _trip_row(t) -> dict:
    values = {c["id"]: t.get(c["id"]) for c in TRIP_COLUMNS}
    return {"key": t["key"], "version": _version(values), "values": values,
            "source": {"recordset": SOURCE_SET, "key": list(t["record_ids"])} if t.get("record_ids") else None}


def _correction_row(c) -> dict:
    values = {col["id"]: c.get(col["id"]) for col in CORRECTION_COLUMNS}
    return {"key": c["correction_id"], "version": _version(values), "values": values, "source": None}


def _findings(result) -> list:
    out = []
    for e in result["excluded"]:
        out.append({"code": "excluded_record", "severity": "info",
                    "message": f"{e['record_id']} ({e['kind']}) is not in any trip: {e['disposition']}, {e['detail']}"})
    for c in result["unapplied_corrections"]:
        unknown = c.get("unknown_record_ids")
        out.append({"code": "unapplied_correction", "severity": "warn",
                    "message": f"correction {c.get('correction_id')} ({c.get('kind')}) changed no trip"
                               + (f"; it names records that are not held: {', '.join(unknown)}" if unknown else "")})
    for rid in result["duplicate_inputs"]:
        out.append({"code": "duplicate_input", "severity": "warn", "message": f"record {rid} was supplied twice and counted once"})
    return out


class _Bad(Exception):
    def __init__(self, detail, field=None):
        super().__init__(detail)
        self.detail, self.field = detail, field


def _params(query):
    today = near = None
    if query.get("today"):
        try:
            today = date.fromisoformat(query["today"])
        except ValueError:
            raise _Bad("today must be an ISO date (YYYY-MM-DD)", "today")
    if query.get("near_home_km"):
        try:
            near = float(query["near_home_km"])
        except ValueError:
            raise _Bad("near_home_km must be a number", "near_home_km")
        if not near >= 0 or near == float("inf"):
            raise _Bad("near_home_km must be a non-negative number", "near_home_km")
    return today, near


def _trips_rows(query):
    today, near = _params(query)
    result, used = asyncio.run(_lane()._compute(today, near))
    trips = trip_engine.order_trips_for_listing(result["trips"])
    return [_trip_row(t) for t in trips], _findings(result), _vintage(TRIPS, used.isoformat(), near, [r for r in map(_trip_row, trips)])


def _correction_rows(query):
    rows = [_correction_row(c) for c in lane_2e_corrections.load_corrections(None, include_retired=True)]
    return rows, [], _vintage(CORRECTIONS, rows)


def _cursor_offset(set_id, token):
    try:
        raw = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
        if raw["s"] != set_id or not isinstance(raw["o"], int) or isinstance(raw["o"], bool) or raw["o"] < 0:
            raise ValueError
        return raw["o"]
    except Exception:  # noqa: BLE001 - every bad cursor reads the same
        raise _Bad("The cursor is not valid for this set.", "cursor")


def _cursor(set_id, offset):
    return base64.urlsafe_b64encode(json.dumps({"s": set_id, "o": offset}, separators=(",", ":")).encode()).decode().rstrip("=")


def _rows_response(set_id, query):
    rows, findings, vintage = (_trips_rows if set_id == TRIPS else _correction_rows)(query)
    start = _cursor_offset(set_id, query["cursor"]) if query.get("cursor") else 0
    end = len(rows)
    if query.get("limit"):
        if not re.fullmatch(r"[0-9]{1,6}", query["limit"]) or int(query["limit"]) < 1:
            raise _Bad("limit must be a positive integer", "limit")
        end = min(len(rows), start + int(query["limit"]))
    if start > len(rows):
        raise _Bad("The cursor is not valid for this set.", "cursor")
    return {"rows": rows[start:end], "vintage": vintage, "stale": None, "findings": findings,
            "next_cursor": _cursor(set_id, end) if end < len(rows) else None}


# --------------------------------------------------------------------------- entry
def _query(event) -> dict:
    qs = event.get("rawQueryString")
    if isinstance(qs, str) and qs:
        return {k: v[-1] for k, v in urllib.parse.parse_qs(qs, keep_blank_values=True).items()}
    return {k: str(v) for k, v in (event.get("queryStringParameters") or {}).items()}


def _path(event) -> str:
    rc = (event.get("requestContext") or {}).get("http") or {}
    path = str(rc.get("path") or event.get("rawPath") or "/")
    return path[len("/default"):] or "/" if path.startswith("/default") else path


def is_records_path(event) -> bool:
    p = _path(event)
    return p == PREFIX or p.startswith(PREFIX + "/")


def handle(event, environ=None):
    """The response for a /records request, or None when the event is not one."""
    if not is_records_path(event):
        return None
    method = str(((event.get("requestContext") or {}).get("http") or {}).get("method") or event.get("httpMethod") or "GET").upper()
    if method == "OPTIONS":  # CORS preflight carries no credential
        return {"statusCode": 204, "headers": {}, "body": "", "isBase64Encoded": False}
    try:
        records_door.authenticate(event.get("headers"), environ)
    except records_door.DoorTokenError as exc:
        print(json.dumps({"records_door_refused": exc.reason}), file=sys.stderr)
        return unauthorized()
    if method != "GET":
        return problem(405, "Method not allowed", extra={"allow": "GET"})
    parts = _path(event)[len(PREFIX):].strip("/").split("/")
    sets = descriptors()
    try:
        if parts == ["recordsets"]:
            return _resp(200, [{"id": d["id"], "title": d["title"]} for d in sets.values()])
        if len(parts) == 3 and parts[0] == "recordsets" and parts[1] in sets and parts[2] == "descriptor":
            return _resp(200, sets[parts[1]])
        if len(parts) == 3 and parts[0] == "recordsets" and parts[1] in sets and parts[2] == "rows":
            return _resp(200, _rows_response(parts[1], _query(event)))
    except _Bad as exc:
        return problem(400, "Invalid request", exc.detail, exc.field)
    except Exception as exc:  # noqa: BLE001 - a lane refusal or a store outage: a problem, never a stack
        code = getattr(exc, "code", None) or type(exc).__name__
        retry = bool(getattr(exc, "retryable", True))
        print(json.dumps({"records_route_error": code}), file=sys.stderr)
        return problem(503 if retry else 409, "Records unavailable", f"The set could not be computed ({code}).")
    return problem(404, "Not found")
