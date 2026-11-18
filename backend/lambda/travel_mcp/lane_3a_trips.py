"""Lane 3A (FLY-TSK-030-0A): list_trips and get_trip. BRD DOC-14994CDDA217 section 6.5, acceptance A-12.

Trips are computed on read and never stored. This module only composes four sources and hands them to the
engine (trip_engine.compute_trips, lane 2D), in this order:

  1. records      flights (flights table), lodging / car rentals / drives (records table): every status,
                  cancelled included, because the engine excludes them with a stated disposition.
  2. home         home_service.read_home (lane 2C), synchronous, so run in a thread. Its window is derived
                  from the records, and it takes no home argument (objective O-4).
  3. home places  lane_2b_home.get_home_places.
  4. corrections  lane_2e_corrections.engine_corrections: active only, presented as the engine expects.

`today` is supplied here (UTC); the engine never reads a clock.

list_trips: contract order via trip_engine.order_trips_for_listing (under way, upcoming, planned, past newest
first). `planned` is not in the contract's list order; lane 2D defines upcoming = starts after today with a known
return, planned = return undecided. We follow that, and planned sorts between upcoming and past.
get_trip: the trip plus, per boundary, its date, basis (contract.BASES), the rule by name (first_rule /
last_rule) and the rule's text; a boundary set by a correction reads `recorded` with that correction listed.
"""
import asyncio
import json
import sys
from datetime import date, datetime, timedelta, timezone

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError

import contract
import home_service
import lane_2a_records
import lane_2b_home
import lane_2e_corrections
import trip_engine

_DATE_FIELDS = ("date_iso", "date", "first_night", "last_night", "pickup_date", "dropoff_date")


class _Refuse(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable


def _lf():
    mod = sys.modules.get("lambda_function")
    if mod is None:
        import lambda_function as mod  # noqa: F401
    return mod


def _fail(exc):
    lf = _lf()
    if exc.code in contract.ERROR_CODES:
        return lf._error(exc.code, exc.message, status=contract.ERROR_CODES[exc.code], retryable=exc.retryable)
    return lf._error(exc.code, exc.message, status=500, retryable=exc.retryable)


def _today():
    return datetime.now(timezone.utc).date()


def _read_home(date_from, date_to):
    """The seam tests replace; production is home_service.read_home, a direct invoke through the function's own role."""
    return home_service.read_home(date_from, date_to)


# --------------------------------------------------------------------------- the four sources
def _scan(table, flt=None):
    kw = {"FilterExpression": flt} if flt is not None else {}
    out = []
    while True:
        page = table.scan(**kw)
        out.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return out
        kw["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _records():
    try:
        flights = _scan(_lf()._get_table())
        rtable, _ = lane_2a_records._table()
        others = _scan(rtable, Attr("kind").is_in(list(lane_2a_records.KINDS)))
    except lane_2a_records._Refuse as exc:
        raise _Refuse(exc.code, exc.message, exc.retryable)
    except (ClientError, BotoCoreError) as exc:
        raise _Refuse("internal_error", f"The travel records could not be read: {exc}", retryable=True)
    return flights + others


def _window(records, today):
    days = [today]
    for r in records:
        for f in _DATE_FIELDS:
            v = r.get(f)
            if isinstance(v, str) and len(v) >= 10:
                try:
                    days.append(date.fromisoformat(v[:10]))
                except ValueError:
                    pass
    return min(days).isoformat(), max(days).isoformat()


def _periods(read):
    out = []
    for p in read.periods:
        if p.end_status == "ended":
            last = p.effective_to - timedelta(days=1)
        elif p.end_status == "end_unknown":
            last = p.effective_from  # home is known for its first night only
        else:
            last = None
        out.append({"residence_uid": p.residence_uid, "first_night": p.effective_from.isoformat(),
                    "last_night": last.isoformat() if last else None, "tenure": p.tenure,
                    "state": p.state, "country": p.country})
    return out


async def _home_places():
    res = await lane_2b_home.get_home_places({})
    body = json.loads(res[0].text)
    if not body.get("success"):
        env = body.get("error_envelope", {})
        raise _Refuse(env.get("code", "missing"), env.get("message", "home places could not be read"), env.get("retryable", False))
    return body["result"]["home_places"]


def _attach_retired(trips, retired):
    """Retired corrections are not engine input; list them on the trips they touched, marked status=retired,
    so a client can see (and not re-retire) them. The trip's dates never depend on them."""
    for t in trips:
        ids = set(t["record_ids"])
        t["corrections"] += [{k: c.get(k) for k in contract.TRIP_CORRECTION_FIELDS}
                             for c in retired if ids & set(c["record_ids"])]


async def _compute():
    today = _today()
    records = await asyncio.to_thread(_records)
    lo, hi = _window(records, today)
    try:
        read = await asyncio.to_thread(_read_home, lo, hi)
    except home_service.HomeError as exc:
        raise _Refuse(exc.code, exc.message, exc.code == "HOME_UNAVAILABLE")
    places = await _home_places()
    try:
        every = await asyncio.to_thread(lane_2e_corrections.load_corrections, None, True)  # retired included
        corrections = [c for c in every if c["status"] == "active"]  # what the engine consumes
    except lane_2e_corrections._Fail as exc:
        raise _Refuse("missing", str(exc))
    try:
        result = trip_engine.compute_trips(records, _periods(read), today=today, home_places=places, corrections=corrections)
        _attach_retired(result["trips"], [c for c in every if c["status"] != "active"])
        result["all_corrections"] = [{k: c.get(k) for k in contract.CORRECTION_READ_FIELDS} for c in every]
        return result, today
    except trip_engine.NoHomeRecorded:
        raise _Refuse("NO_HOME_RECORDED", "No residence period is recorded in finance, so no trip can be told from home.")


# --------------------------------------------------------------------------- handlers
def _iso(field, v):
    try:
        return date.fromisoformat(str(v).strip()[:10]).isoformat()
    except ValueError:
        raise _Refuse("invalid", f"{field} must be an ISO date (YYYY-MM-DD), got {v!r}")


async def list_trips(arguments):
    try:
        lo = _iso("date_from", arguments["date_from"]) if arguments.get("date_from") else None
        hi = _iso("date_to", arguments["date_to"]) if arguments.get("date_to") else None
        result, today = await _compute()
        trips = trip_engine.order_trips_for_listing(trip_engine.trips_in_range(result["trips"], lo, hi))
        return _lf()._success({"trips": trips, "count": len(trips), "today": today.isoformat(),
                               "excluded": result["excluded"], "unapplied_corrections": result["unapplied_corrections"],
                               "corrections": result["all_corrections"]})
    except _Refuse as exc:
        return _fail(exc)


def _boundary(trip, which):
    rule = trip.get(f"{which}_rule")
    spec = contract.RULES.get(rule) or {}
    basis = trip[f"{which}_basis"]
    out = {"date": trip[f"{which}_date"], "basis": basis, "rule": rule, "rule_text": spec.get("text"),
           "evidence_date": trip.get(f"{which}_evidence_date")}
    if basis == "recorded":
        # the corrections that set this boundary: `left` sets the first, `returned` the last (engine shape)
        kind = "left" if which == "first" else "returned"
        out["corrections"] = [c for c in trip.get("corrections") or [] if c.get("kind") == kind and c.get("status") == "active"]
    return out


async def get_trip(arguments):
    try:
        key = arguments.get("trip_key")
        if not isinstance(key, str) or not key.strip():
            raise _Refuse("invalid", "trip_key is required")
        result, today = await _compute()
        trip = next((t for t in result["trips"] if t["key"] == key.strip()), None)
        if trip is None:
            raise _Refuse("missing", f"No trip has the key {key!r}. A trip key is the id of its first record; "
                                     "trips are computed on read, so a key can change when records or corrections change.")
        return _lf()._success({**trip, "boundaries": {"first": _boundary(trip, "first"), "last": _boundary(trip, "last")},
                               "today": today.isoformat()})
    except _Refuse as exc:
        return _fail(exc)


HANDLERS = {"list_trips": list_trips, "get_trip": get_trip}
