"""Lane 2B (FLY-TSK-029-0B): home places per residence and their two tools.

Tools: get_home_places, set_home_places. One record per residence_uid
(pk = home_places#<hash16>, sk = "rec"; keys from contract.record_keys).

Place identity is DOC-76D85C855CBE s5: a home place stores place_id, area_id, label and an optional
radius_km; the record carries valid_from/valid_to. A place is HOME when its place_id is listed or its
area_id equals a listed home area_id, inside the date range.

"Is this place home" (A-7, second half) -- the rule implemented by `evaluate_place`, which lane 3B calls:
  * place_id listed, or area_id equal to a listed home area_id   -> home, no question.
  * otherwise, if a listed home place has radius_km and lat/lon and the place has lat/lon and lies within
    radius_km of it (haversine)                                   -> NEAR HOME, NOT LISTED: raise the
    registered question contract.QUESTIONS["is this place home"] (q_is_this_place_home).
  * otherwise (no radius_km, or farther away, or no coordinates)  -> not home, no question; where radius_km
    is absent the rule is plain same-area_id equality.
q_is_this_place_home carries travel-side answers, so it is NOT in contract.QUESTIONS_WITHOUT_TRAVEL_ANSWER.
Wording is lane 3B's; the text/labels here are placeholders with the contract's field names.
"""
import json
import math
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal

import contract
import settings as _settings

KIND = "home_places"
QUESTION_ID = contract.QUESTIONS["is this place home"]
assert QUESTION_ID not in contract.QUESTIONS_WITHOUT_TRAVEL_ANSWER

_PLACE_RE = re.compile(contract.PLACE_ID_PATTERN)
_AREA_RE = re.compile(contract.AREA_ID_PATTERN)


# ------------------------------------------------------------------ result helpers (no lambda_function import)
def _text(payload):
    from mcp.types import TextContent
    return [TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


def _ok(data):
    return _text({"success": True, "result": _native(data)})


def _err(code, message, retryable=False):
    assert code in contract.ERROR_CODES
    return _text({"success": False, "error": message,
                  "error_envelope": {"code": code, "message": message, "retryable": retryable}})


def _native(v):
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, dict):
        return {k: _native(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_native(x) for x in v]
    return v


def _now():
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------ table access
class _TableUnavailable(Exception):
    pass


_resource = None


def _table():
    """The records table, named only by RECORDS_TABLE_NAME. Never created, never defaulted."""
    global _resource
    name = os.environ.get("RECORDS_TABLE_NAME", "").strip()
    if not name:
        raise _TableUnavailable("the RECORDS_TABLE_NAME setting is not set on this deployment, so home places cannot be read or written yet")
    import boto3
    if _resource is None:
        try:
            region = _settings.region()
        except _settings.SettingsError as exc:  # no default region exists (A-14): fail closed
            raise _TableUnavailable(f"{exc}, so home places cannot be read or written")
        _resource = boto3.resource("dynamodb", region_name=region)
    return _resource.Table(name)


def _unavailable(exc):
    return _err("missing", str(exc) if isinstance(exc, _TableUnavailable) else
                f"the records table named by RECORDS_TABLE_NAME is not reachable: {exc}",
                retryable=not isinstance(exc, _TableUnavailable))


def _guard_call(fn):
    from botocore.exceptions import BotoCoreError, ClientError
    try:
        return fn(), None
    except _TableUnavailable as exc:
        return None, _unavailable(exc)
    except (ClientError, BotoCoreError) as exc:
        return None, _unavailable(exc)


# ------------------------------------------------------------------ validation
def _iso(v, field):
    if v in (None, ""):
        return None
    try:
        date.fromisoformat(str(v))
    except ValueError:
        raise ValueError(f"{field} must be an ISO date (YYYY-MM-DD)")
    return str(v)


def _number(v, field):
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError(f"{field} must be a number")
    return v


def _clean_place(p, i):
    if not isinstance(p, dict) or not _PLACE_RE.match(str(p.get("place_id", ""))):
        raise ValueError(f"places[{i}].place_id must look like type:code with type in {'|'.join(contract.PLACE_TYPES)}")
    pid = p["place_id"]
    area = p.get("area_id") or (pid if pid.startswith("city:") else None)
    if not area or not _AREA_RE.match(area):
        raise ValueError(f"places[{i}].area_id (city:<slug>) is required for a non-city place such as {pid}")
    out = {"place_id": pid, "area_id": area, "label": p.get("label") or pid}
    for f in ("lat", "lon"):
        if p.get(f) is not None:
            out[f] = _number(p[f], f"places[{i}].{f}")
    if p.get("radius_km") is not None:
        r = _number(p["radius_km"], f"places[{i}].radius_km")
        if r <= 0:
            raise ValueError(f"places[{i}].radius_km must be greater than 0")
        if "lat" not in out or "lon" not in out:
            raise ValueError(f"places[{i}].radius_km needs lat and lon on the same place")
        out["radius_km"] = r
    return out


def _dec(v):
    return json.loads(json.dumps(v), parse_float=Decimal)


# ------------------------------------------------------------------ handlers
def _session(args):
    """The session stamp: the tool argument wins, else the internal door's event session (lambda_function.stamp_session)."""
    try:
        import lambda_function as lf
        return lf.stamp_session(args)
    except Exception:
        return str(args.get("session") or "")


def _author():
    try:
        import lambda_function as lf
        return (lf._PRINCIPAL.get() or {}).get("id") or "unknown"
    except Exception:
        return "unknown"


async def set_home_places(args):
    args = dict(args or {})
    dry_run = args.pop("dry_run", None) is True
    args.pop("idempotency_key", None)
    uid = str(args.get("residence_uid") or "").strip()
    if not uid:
        return _err("invalid", "residence_uid is required")
    places = args.get("places")
    if not isinstance(places, list) or not places:
        return _err("invalid", "places must be a non-empty list")
    try:
        clean = [_clean_place(p, i) for i, p in enumerate(places)]
        vf, vt = _iso(args.get("valid_from"), "valid_from"), _iso(args.get("valid_to"), "valid_to")
        if vf and vt and vt < vf:
            raise ValueError("valid_to is before valid_from")
    except ValueError as exc:
        return _err("invalid", str(exc))
    keys = contract.record_keys(KIND, [uid])
    now = _now()

    def run():
        tbl = _table()
        prior = tbl.get_item(Key={"pk": keys["pk"], "sk": keys["sk"]}).get("Item")
        item = {**keys, "kind": KIND, "residence_uid": uid, "places": _dec(clean), "status": "active",
                "source": args.get("source") or "set_home_places", "author": _author(),
                "created_at": (prior or {}).get("created_at", now), "updated_at": now}
        if _session(args):
            item["session"] = _session(args)
        if vf:
            item["valid_from"] = item["date_iso"] = vf
        if vt:
            item["valid_to"] = vt
        if not dry_run:
            tbl.put_item(Item=item)
        return item, prior is not None

    res, bad = _guard_call(run)
    if bad:
        return bad
    item, replaced = res
    out = {"home_places": item, "replaced": replaced}
    if dry_run:
        out["dry_run"] = True
    return _ok(out)


async def get_home_places(args):
    uid = str((args or {}).get("residence_uid") or "").strip()

    def run():
        tbl = _table()
        if uid:
            k = contract.record_keys(KIND, [uid])
            it = tbl.get_item(Key={"pk": k["pk"], "sk": k["sk"]}).get("Item")
            return [it] if it else []
        from boto3.dynamodb.conditions import Attr
        items, kw = [], {"FilterExpression": Attr("kind").eq(KIND)}
        while True:
            r = tbl.scan(**kw)
            items += r.get("Items", [])
            if "LastEvaluatedKey" not in r:
                return items
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    res, bad = _guard_call(run)
    if bad:
        return bad
    return _ok({"home_places": res, "count": len(res)})


HANDLERS = {"get_home_places": get_home_places, "set_home_places": set_home_places}


# ------------------------------------------------------------------ the "is this place home" rule (lane 3B)
def _haversine_km(a_lat, a_lon, b_lat, b_lon):
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b_lon - a_lon) / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(h))


def _in_range(rec, on_date):
    d = str(on_date) if on_date else None
    return d is None or ((not rec.get("valid_from") or rec["valid_from"] <= d) and
                         (not rec.get("valid_to") or d <= rec["valid_to"]))


def evaluate_place(home_records, place, on_date=None, trip_key=None):
    """Classify one place against home-places records (from get_home_places).

    place: {place_id, area_id, lat?, lon?, label?}. Returns {"status": "home"|"not_home"|"ask", "question": ...}.
    status "ask" carries the registered q_is_this_place_home question; every other status carries None.
    """
    home_places = [hp for rec in home_records if _in_range(rec, on_date) for hp in rec.get("places", [])]
    if any(hp["place_id"] == place.get("place_id") or (place.get("area_id") and hp["area_id"] == place["area_id"])
           for hp in home_places):
        return {"status": "home", "question": None}
    lat, lon = place.get("lat"), place.get("lon")
    if lat is not None and lon is not None:
        for hp in home_places:
            r = hp.get("radius_km")
            if r is not None and hp.get("lat") is not None and hp.get("lon") is not None:
                km = _haversine_km(float(hp["lat"]), float(hp["lon"]), float(lat), float(lon))
                if km <= float(r):
                    return {"status": "ask", "question": is_this_place_home_question(trip_key, place, hp, km)}
    return {"status": "not_home", "question": None}


def is_this_place_home_question(trip_key, place, near_home, distance_km):
    """The registered question (contract.QUESTIONS['is this place home']) with contract.QUESTION_FIELDS."""
    name = place.get("label") or place.get("place_id")
    answers = [
        {"answer_id": "home", "label": "Yes, it counts as home", "correction": {"kind": "returned"}},
        {"answer_id": "away", "label": "No, it is away from home", "correction": {"kind": "left"}},
        {"answer_id": "leave_out", "label": "Leave it out of the trip", "correction": {"kind": "leave_out"}},
    ]
    assert contract.QUESTION_ANSWERS_MIN <= len(answers) <= contract.QUESTION_ANSWERS_MAX
    return {"question_id": QUESTION_ID, "trip_key": trip_key,
            "text": f"{name} is about {distance_km:.0f} km from {near_home.get('label') or near_home['place_id']} and is not "
                    f"listed as a home place. Is it home?",
            "answers": answers}
