"""@io-kit/records routes (FLY-TSK-083, FLY-FTR-004; write half FLY-TSK-087): travel proves the records contract is general.

  GET  /records/recordsets
  GET  /records/recordsets/{id}/descriptor
  GET  /records/recordsets/{id}/rows       [?today=YYYY-MM-DD&near_home_km=N for trips] [?limit=N&cursor=...]
  GET  /records/recordsets/{id}/writes     the corrections' create and retire events (records_writes)
  POST /records/recordsets/trip_corrections/intents   create | retire | undo, through the two tool handlers only

Two sets, both served from what the tools already read; there is no business logic here, only shape:
  trips             class derived, order fixed. Rows are lane_3a_trips._compute (trip_engine.compute_trips) ordered by
                    trip_engine.order_trips_for_listing. Params are the engine's set-level parameters (today, near_home_km).
                    Each row's source lists its record ids; excluded records, unapplied corrections and duplicate inputs
                    are findings.
  trip_corrections  class amend_with_audit, capabilities create and retire, reason required: lane_2e_corrections.
                    load_corrections, retired ones included. Writes go through record_trip_correction and
                    retire_trip_correction (records_writes); this module writes nothing itself.

Auth: a human-door token only (records_door.authenticate), any think sign-in (io, 2026-11-24: "Any think sign-in"; no
per-subject list). Every refusal, descriptors included, is the same 401 problem.
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

import contract
import lane_2e_corrections
import records_door
import records_writes
import trip_engine

PREFIX = "/records"
JSON = "application/json"
PROBLEM = "application/problem+json"

SOURCE_SET = "travel_records"  # lineage target: the underlying travel records; not served as a set, so an {external} input
TRIPS, CORRECTIONS = "trips", "trip_corrections"


def _col(id_, label, type_, options=None, editable=False, **hint):
    c = {"id": id_, "label": label, "type": type_, "editable": editable}
    if options is not None:
        c["options"] = options
    if hint:
        c["hint"] = hint
    return c


# Inline options (FLY-TSK-088, BRD DOC-D14184D324F0 6.7): every enum column serves its values with travel's own glosses.
# The values come from the contract's vocabularies, so a value added there without a gloss fails the import, not a view.
# Wording: the design canvas's Travel board (lowercase is in the strings, never CSS).
_GLOSSES = {
    "kind": {"returned": "i came home on a date no record shows", "left": "i set out on a date no record shows",
             "together": "these records are one trip", "leave_out": "this is not a trip",
             "confirmed": "the engine has it right. stop asking."},
    "method": {"examined": "looked at the records", "accepted_list": "accepted from the list"},
    "basis": {"recorded": "a record shows it", "derived": "worked out by a rule", "undecided": "not settled"},
    "state": {"under way": "under way", "upcoming": "upcoming", "planned": "planned, dates not settled", "past": "past"},
}
_VALUES = {"kind": contract.CORRECTION_KINDS, "method": contract.CONFIRMATION_METHODS,
           "basis": contract.BASES, "state": tuple(trip_engine.STATE_ORDER)}


def _options(vocab):
    glosses = _GLOSSES[vocab]
    missing = [v for v in _VALUES[vocab] if v not in glosses]
    if missing or set(glosses) - set(_VALUES[vocab]):
        raise RuntimeError(f"records options for {vocab} do not match the contract: {sorted(set(glosses) ^ set(_VALUES[vocab]))}")
    return [{"value": v, "label": glosses[v]} for v in _VALUES[vocab]]


TRIP_COLUMNS = [
    _col("key", "trip", "text"),
    _col("first_date", "first date", "date"), _col("first_basis", "first date basis", "enum", _options("basis")),
    _col("last_date", "last date", "date"), _col("last_basis", "last date basis", "enum", _options("basis")),
    _col("nights", "nights", "number", align="end"), _col("nights_exact", "nights exact", "boolean"),
    _col("state", "state", "enum", _options("state")),
    _col("places_visited", "places visited", "json", default_hidden=True),
    _col("flags", "flags", "json"), _col("signals", "signals", "json"),
    _col("legs", "legs", "json", default_hidden=True), _col("stays", "stays", "json", default_hidden=True),
    _col("rentals", "rentals", "json", default_hidden=True),
    _col("corrections", "corrections", "json", default_hidden=True),
    _col("record_ids", "record ids", "json", default_hidden=True),
]
CORRECTION_COLUMNS = [
    # editable marks the fields a correction is created with (records_writes.CREATE_FIELDS); nothing is amended.
    # A retired correction is served with Row.retired true (contract 0.5.0) and its values unchanged, so the row
    # carries no status, retired_at or retired_by: the retire is the writes log's entry (FLY-TSK-089).
    _col("correction_id", "correction", "text"), _col("kind", "kind", "enum", _options("kind"), True),
    _col("date", "date", "text", editable=True), _col("date_iso", "date (iso)", "date"),
    _col("record_ids", "record ids", "json", editable=True), _col("reason", "reason", "text"),
    _col("author", "author", "text"), _col("session", "session", "text"), _col("question_id", "question", "text", editable=True),
    _col("created_at", "created", "text"),
    _col("first_date", "first date", "date", editable=True), _col("last_date", "last date", "date", editable=True),
    _col("method", "method", "enum", _options("method"), True), _col("supersedes", "supersedes", "json", editable=True),
]
# Descriptor.description (contract 0.4.0): one sentence a host may show under the title, in the canvas's voice.
TRIPS_DESCRIPTION = ("every trip the engine can assemble from my flights, stays, rentals and drives. where it gets one wrong, "
                     "i don't edit the trip. i leave a correction, and the trip is derived again.")
CORRECTIONS_DESCRIPTION = ("the corrections i left on trips, each with its reason. a retired one stays listed, "
                           "and its trip goes back to what the records say.")
# Param.label arrives in the W2 kit release (lane 2H). Until travel's gate runs on that release, a served label would fail
# the contract (Param takes no other property), so the labels are held here and switched on with the kit bump.
PARAM_LABELS = {"today": "today", "near_home_km": "near home, km"}
SERVE_PARAM_LABELS = False


def _params_served(params):
    return [{**p, "label": PARAM_LABELS[p["id"]]} if SERVE_PARAM_LABELS else p for p in params]


_NEAR_HOME_DEFAULT = inspect.signature(trip_engine.compute_trips).parameters["near_home_km"].default


def _lane():
    mod = sys.modules.get("lane_3a_trips")
    if mod is None:
        import lane_3a_trips as mod  # noqa: F401
    return mod


def descriptors() -> dict:
    today = _lane()._today().isoformat()
    return {
        TRIPS: {"id": TRIPS, "title": "trips", "description": TRIPS_DESCRIPTION, "key": "key", "class": "derived", "capabilities": [], "reason": "none",
                "collection": "client", "order": "fixed", "inputs": [{"external": SOURCE_SET, "label": "Travel records"}, CORRECTIONS],
                "params": _params_served([{"id": "today", "type": "date", "options_from": None, "default": today},
                                          {"id": "near_home_km", "type": "number", "options_from": None,
                                           "default": _NEAR_HOME_DEFAULT}]),
                "columns": TRIP_COLUMNS},
        CORRECTIONS: {"id": CORRECTIONS, "title": "trip corrections", "description": CORRECTIONS_DESCRIPTION, "key": "correction_id", "class": "amend_with_audit",
                      "capabilities": ["create", "retire"], "reason": "required", "collection": "client", "order": "free",
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
    # Contract amendment 1: `sources` names every travel record the trip was built from, one {recordset, key} per record;
    # `source` is the deprecated alias for the first of them (null when the trip has no lineage).
    sources = [{"recordset": SOURCE_SET, "key": str(rid)} for rid in (t.get("record_ids") or [])]
    return {"key": t["key"], "version": _version(values), "values": values,
            "sources": sources, "source": sources[0] if sources else None}


def _correction_row(c) -> dict:
    values = {col["id"]: c.get(col["id"]) for col in CORRECTION_COLUMNS}
    row = {"key": c["correction_id"], "version": _version(values), "values": values, "sources": [], "source": None}
    if c.get("status") == "retired":  # listed, dimmed by the view; the values stay as they were, the version moves
        row["retired"] = True
        row["version"] = _version({"values": values, "retired": True})
    return row


# One finding per code, its message a short summary (lead ruling 18:09Z, BRD canvas "Trips"): a panel shows the
# summary, not one line per record. Finding.rows names keys of THIS response only (REC-C44), and the records a trips
# finding speaks of are not trip rows, so no rows are listed; the summary counts them.
_KIND_PLURAL = {"flight": "flights", "lodging": "stays", "car_rental": "car rentals", "drive": "drives"}
_BEFORE = {"cancelled": "cancelled", "unreadable": "unreadable", "left_out": "left-out"}
_AFTER = {"at_home": "at home", "a_move": "in a move", "day_out_and_back": "out and back in a day"}


def _plural(n, word, many):
    return f"{n} {word if n == 1 else many}"


def _excluded_part(n, kind, disposition):
    noun = _KIND_PLURAL.get(kind, f"{kind} records") if n != 1 else {"car_rental": "car rental", "lodging": "stay"}.get(kind, kind)
    if disposition in _BEFORE:
        return f"{n} {_BEFORE[disposition]} {noun}"
    return f"{n} {noun} {_AFTER.get(disposition, disposition.replace('_', ' '))}"


def _findings(result) -> list:
    out = []
    excluded = result["excluded"]
    if excluded:
        groups = {}
        for e in excluded:
            groups[(e["disposition"], e["kind"])] = groups.get((e["disposition"], e["kind"]), 0) + 1
        parts = [_excluded_part(n, kind, disp) for (disp, kind), n in sorted(groups.items(), key=lambda g: (-g[1], g[0]))]
        msg = f"{parts[0]} {'is' if len(excluded) == 1 else 'are'} in no trip" if len(parts) == 1 else \
            f"{_plural(len(excluded), 'record is', 'records are')} in no trip: {', '.join(parts)}"
        out.append({"code": "excluded_record", "severity": "info", "message": msg})
    unapplied = result["unapplied_corrections"]
    if unapplied:
        unknown = sum(1 for c in unapplied if c.get("unknown_record_ids"))
        msg = f"{_plural(len(unapplied), 'correction', 'corrections')} changed no trip"
        if unknown:
            msg += f"; {unknown} {'names' if unknown == 1 else 'name'} records that are not held"
        out.append({"code": "unapplied_correction", "severity": "warn", "message": msg})
    dup = result["duplicate_inputs"]
    if dup:
        out.append({"code": "duplicate_input", "severity": "warn",
                    "message": f"{_plural(len(dup), 'record was', 'records were')} supplied twice and counted once"})
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
        claims = records_door.authenticate(event.get("headers"), environ)
    except records_door.DoorTokenError as exc:
        print(json.dumps({"records_door_refused": exc.reason}), file=sys.stderr)
        return unauthorized()
    parts = _path(event)[len(PREFIX):].strip("/").split("/")
    sets = descriptors()
    if method == "POST" and len(parts) == 3 and parts[0] == "recordsets" and parts[1] in sets and parts[2] == "intents":
        try:
            return records_writes.intent(sys.modules[__name__], parts[1], event, claims)
        except Exception as exc:  # noqa: BLE001 - a store outage: a problem, never a stack
            print(json.dumps({"records_write_error": type(exc).__name__}), file=sys.stderr)
            return problem(503, "Records unavailable", "The write could not be made.")
    if method != "GET":
        return problem(405, "Method not allowed", extra={"allow": "GET, POST" if parts[-1:] == ["intents"] else "GET"})
    try:
        if len(parts) == 3 and parts[0] == "recordsets" and parts[1] == records_writes.SET and parts[2] == "writes":
            return _resp(200, records_writes.writes(sys.modules[__name__], parts[1], claims))
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
