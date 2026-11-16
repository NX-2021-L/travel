"""The travel contract (FLY-TSK-028-0A, BRD DOC-14994CDDA217 section 7.2 lane 0A).

Single source of truth for names and schemas: record kinds and their fields, the 24 tool
names and the schemas of the 19 new ones, the basis / flag / signal / question vocabularies,
the error codes, the settings a deployment supplies, and the author stamp.

This module is data plus two tiny key helpers. It imports nothing but the standard library
and names no io instance value: a deployment supplies the VALUES of SETTINGS in its environment.
The five flight tools' schemas stay in lambda_function.py, unchanged; they are pinned by
tests/test_contract.py against tests/fixtures/flight_tools_4e0cc8d.json.
"""
import hashlib

CONTRACT_VERSION = "1.0.0"

# --------------------------------------------------------------------------- settings
# KEYS only. Every value comes from the deployment's environment (BRD B1, A-2).
# "required" is True (every door), False (never), or the name of the DOOR_MODE that requires it: "public"
# means required on a public edge and optional behind the internal door, the convention PUBLIC_BASE_URL and
# CORS_ORIGINS already follow (FLY-TSK-043). settings.missing_required() is the one reader.
SETTINGS = {
    "TABLE_NAME": {"required": True, "purpose": "the flights table"},
    "RECORDS_TABLE_NAME": {"required": True, "purpose": "the records table (hash pk, range sk, GSI area-index)"},
    "SURFACE_NAME": {"required": True, "purpose": "the surface and connector name this deployment serves"},
    "FINANCE_READ_FUNCTION": {"required": "public", "purpose": "finance's HTTPS Function URL, called over HTTPS for the residence read; finance refuses a direct Lambda invoke (finding DOC-4CEAAE4B8CB7)"},
    "FINANCE_READ_CREDENTIAL": {"required": False, "purpose": "the bearer credential for that Function URL call; never committed, provisioned on the function by its owner; absent, a home read refuses HOME_UNAVAILABLE (cause config)"},
    "COGNITO_USER_POOL_ID": {"required": "public", "purpose": "sign-in pool"},
    "COGNITO_CLIENT_ID": {"required": "public", "purpose": "sign-in client"},
    "COGNITO_REGION": {"required": "public", "purpose": "sign-in pool region"},
    "COGNITO_ALLOWED_SUBS": {"required": "public", "purpose": "principals allowed to call"},
    "IOGRAPH_ALLOWED_SUBS": {"required": False, "purpose": "second issuer's allowed principals"},
    "TRAVEL_WRITE_GROUPS": {"required": False, "purpose": "groups allowed to write"},
    "PUBLIC_BASE_URL": {"required": False, "purpose": "public edge base URL; absent behind the internal door"},
    "CORS_ORIGINS": {"required": False, "purpose": "allowed browser origins; absent behind the internal door"},
}
FIXED_SETTINGS = {"MCP_TRANSPORT": "streamable_http"}  # not instance-naming

# --------------------------------------------------------------------------- places (DOC-76D85C855CBE s5)
PLACE_TYPES = ("apt", "stn", "city", "addr")
PLACE_ID_PATTERN = r"^(apt|stn|city|addr):[A-Za-z0-9._-]+$"
AREA_ID_PATTERN = r"^city:[a-z0-9-]+$"
PLACE_RESOLUTIONS = ("exact", "geocoded", "unresolved")

# --------------------------------------------------------------------------- author stamp
# Server-stamped from the authenticated principal; never accepted as tool input except `session`.
AUTHOR_STAMP = {
    "fields": {"author": {"required": True, "type": "string"}, "session": {"required": False, "type": "string"}},
    "input_arg": "session",
    "applies_to": "every record on create; corrections also stamp retired_by on retire",
}

# --------------------------------------------------------------------------- records table keys
RECORDS_TABLE = {
    "hash_key": "pk", "range_key": "sk",
    "pk": "<kind>#<hash16>   hash16 = natural_key_hash(kind, natural-key values)",
    "sk": "rec",  # the current-state row; every kind, one row per record
    "record_id": "<kind>-<hash16>",
    "gsi": {"area-index": {"hash": "area_id", "range": "date_iso"}},
    "common_attributes": ["pk", "sk", "record_id", "kind", "status", "date_iso", "created_at", "updated_at",
                          "source", "author", "session"],
    "listing_a_kind": "a Scan filtered on attribute `kind` (the table is a few hundred rows); no kind index in the template",
}
SK_CURRENT = "rec"
STATUSES = ("active", "cancelled")

_PLACE_PAIR = ("place_id", "area_id")
RECORD_KINDS = {
    "lodging": {
        "natural_key": ["first_night", "name", "city", "state", "country"],
        "fields": ["name", "city", "state", "country", "address", "first_night", "last_night", "confirmation_code",
                   "status", "source", "place_id", "area_id", "place_label", "lat", "lon", "place_resolution"],
        "required": ["name", "city", "state", "country", "first_night", "last_night"],
        "mutable": ["last_night", "address", "confirmation_code", "source", "place_label", "lat", "lon"],
        "date_iso": "first_night", "index_area_id": "area_id", "place_ends": {"": _PLACE_PAIR},
    },
    "car_rental": {
        "natural_key": ["pickup_date", "pickup_place", "company", "confirmation_code"],
        "fields": ["company", "pickup_place", "pickup_date", "dropoff_place", "dropoff_date", "confirmation_code",
                   "status", "source", "pickup_place_id", "pickup_area_id", "return_place_id", "return_area_id",
                   "place_resolution"],
        "required": ["company", "pickup_place", "pickup_date"],
        "mutable": ["dropoff_place", "dropoff_date", "source"],
        "date_iso": "pickup_date", "index_area_id": "pickup_area_id",
        "place_ends": {"pickup": _PLACE_PAIR, "return": _PLACE_PAIR},
    },
    "drive": {
        "natural_key": ["date", "origin", "destination", "sequence"],
        "fields": ["date", "origin", "destination", "via", "means", "sequence", "status", "source",
                   "from_place_id", "from_area_id", "to_place_id", "to_area_id", "from_lat", "from_lon",
                   "to_lat", "to_lon", "place_resolution"],
        "required": ["date", "origin", "destination"],
        "mutable": ["via", "means", "source"],
        "date_iso": "date", "index_area_id": "from_area_id",
        "place_ends": {"from": _PLACE_PAIR, "to": _PLACE_PAIR},
    },
    "home_places": {  # one row per residence_uid (BRD 6.4; DOC-76D85C855CBE s5 lane 2B)
        "natural_key": ["residence_uid"],
        "fields": ["residence_uid", "valid_from", "valid_to", "places", "status", "source"],
        "places_item_fields": ["place_id", "area_id", "label", "radius_km"],
        "required": ["residence_uid", "places"],
        "mutable": ["valid_from", "valid_to", "places"],
        "date_iso": "valid_from", "index_area_id": None, "place_ends": {},
    },
    "correction": {  # BRD 6.6
        "natural_key": ["kind", "date", "record_ids(sorted, comma-joined)", "created_at"],
        "fields": ["kind", "date", "record_ids", "reason", "author", "session", "question_id", "status",
                   "created_at", "retired_at", "retired_by"],
        "required": ["kind", "record_ids"],
        "mutable": ["status", "retired_at", "retired_by"],  # retire only; a correction is never deleted
        "date_iso": "date", "index_area_id": None, "place_ends": {},
    },
}
DRIVE_MEANS = ("road",)  # rail, sea, other are later values
CORRECTION_KINDS = ("returned", "left", "together", "leave_out")
CORRECTION_STATUSES = ("active", "retired")


def natural_key_hash(kind, values):
    """First 16 hex of sha256 over the kind and the stripped, case-folded natural-key values."""
    parts = [kind] + [str(v).strip().casefold() for v in values]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:16]


def record_keys(kind, values):
    h = natural_key_hash(kind, values)
    return {"pk": f"{kind}#{h}", "sk": SK_CURRENT, "record_id": f"{kind}-{h}"}


# --------------------------------------------------------------------------- vocabularies (BRD 6.5, 6.6)
# The name is BASES (a boundary's basis is one of these); there is no `BASIS`. Chosen over adding an alias: every
# shipped module and test already says BASES, and one name is one vocabulary.
BASES = ("recorded", "derived", "undecided")
# Rules by name. derives_boundary: may be the rule behind a `derived` first/last boundary (get_trip names it).
RULES = {
    "home_to_home": {"derives_boundary": False, "text": "a trip runs from the first movement that leaves home to the next that arrives home"},
    "one_line": {"derives_boundary": False, "text": "flights, drives, lodging and car rentals are laid on the same line of dates"},
    "no_flight_needed": {"derives_boundary": False, "text": "a stay or a drive away from home is a trip with no flight"},
    "from_lodging": {"derives_boundary": True, "text": "no recorded way back ends on the last check-out; no recorded way out begins on the first check-in"},
    "forced": {"derives_boundary": True, "text": "when the records leave one possible date for a boundary, it is taken"},
    # `bounded` is the rule behind an UNDECIDED boundary that still shows a bound (BRD 5.3); it never makes a
    # `derived` one. The engine emits basis `undecided` with rule `bounded`: the contract now says so.
    "bounded": {"derives_boundary": False, "text": "an end that cannot be derived is back by the next departure from home"},
    "connection_or_visit": {"derives_boundary": False, "text": "a place left the day it was reached is a connection"},
    "a_move": {"derives_boundary": True, "text": "arriving at a new residence on or after its first night is arriving home"},
}
# Why a record sits in no trip. One disposition per excluded record (trip_engine `excluded` entries).
DISPOSITIONS = ("cancelled", "left_out", "unreadable", "at_home", "a_move", "day_out_and_back")
FLAGS = ("start not recorded", "return not recorded", "return not planned yet",
         "home not recorded for these dates", "is this place home")
SIGNALS = ("likely duplicate", "unusually long")
TRIP_STATES = ("planned", "upcoming", "under way", "past")
# upcoming = starts after today with a KNOWN return; planned = starts after today with the return still undecided
# (trip_engine.trip_state, lane 3A). Both are states; `planned` sorts between upcoming and past.
TRIP_STATE_MEANING = {
    "under way": "has begun and has not ended",
    "upcoming": "has not begun; the return is known",
    "planned": "has not begun; the return is undecided",
    "past": "has ended",
}
TRIP_LIST_ORDER = ("under way", "upcoming", "planned", "past")  # past newest first
TRIP_FIELDS = ["key", "first_date", "first_basis", "last_date", "last_basis", "nights", "places_visited", "legs",
               "stays", "rentals", "state", "flags", "signals", "residence_period", "corrections"]
GET_TRIP_EXTRA_FIELDS = ["first_rule", "last_rule"]  # the rule behind each derived boundary

# Questions: one per flag and per signal. Ids are registered here; WORDING is io's vocabulary
# and lives in one file written with io (lane 3B), not in the contract.
QUESTIONS = {
    "start not recorded": "q_start_not_recorded",
    "return not recorded": "q_return_not_recorded",
    "return not planned yet": "q_return_not_planned_yet",
    "home not recorded for these dates": "q_home_not_recorded",
    "is this place home": "q_is_this_place_home",
    "likely duplicate": "q_likely_duplicate",
    "unusually long": "q_unusually_long",
}
QUESTION_ANSWERS_MIN, QUESTION_ANSWERS_MAX = 2, 4
QUESTIONS_WITHOUT_TRAVEL_ANSWER = ("q_home_not_recorded",)  # sends the person to finance; no answers
QUESTION_FIELDS = ["question_id", "trip_key", "text", "answers"]
ANSWER_FIELDS = ["answer_id", "label", "correction"]  # correction: the correction it would write

# --------------------------------------------------------------------------- errors
# status per BRD 6.3 / DOC-4CEAAE4B8CB7 (HOME_UNAVAILABLE, NO_HOME_RECORDED fixed; no HOME_UNAUTHORIZED).
# Statuses for the two HOME_* codes and not_implemented are this lane's choice.
ERROR_CODES = {
    "duplicate": 409, "invalid": 400, "not_allowed": 403, "missing": 404,
    "HOME_UNAVAILABLE": 503, "NO_HOME_RECORDED": 422, "not_implemented": 501,
}
LEGACY_ERROR_CODES = ("invalid_next_token", "missing_param", "not_found", "no_updates", "invalid_date",
                      "unknown_tool", "forbidden", "internal_error")  # flight tools and dispatcher, unchanged

# --------------------------------------------------------------------------- tools
FLIGHT_TOOLS = ("search_flights", "get_flight", "create_flight", "update_flight", "cancel_flight")


def _p(t, d=None, **kw):
    s = {"type": t}
    if d:
        s["description"] = d
    s.update(kw)
    return s


def _obj(props, required=()):
    s = {"type": "object", "properties": props}
    if required:
        s["required"] = list(required)
    return s


def _S(d=None, **kw):
    return _p("string", d, **kw)


_RANGE = {"date_from": _S("Start date (ISO 8601)"), "date_to": _S("End date (ISO 8601), inclusive")}
_SESSION = {"session": _S("Optional agent session id, recorded in the author stamp")}
_KINDS = ["lodging", "car_rental", "drive"]

_LODGING = {
    "name": _S("Property name"), "city": _S("City"), "state": _S("State or region"), "country": _S("Country"),
    "address": _S("Street address; stored as given, never parsed"),
    "first_night": _S("First night (ISO date); nights are inclusive"), "last_night": _S("Last night (ISO date)"),
    "confirmation_code": _S(), "source": _S("Where the record came from"),
    "place_label": _S("Place as booked"), "lat": _p("number"), "lon": _p("number"),
}
_CAR = {
    "company": _S(), "pickup_place": _S(), "pickup_date": _S("ISO date"),
    "dropoff_place": _S(), "dropoff_date": _S("ISO date"), "confirmation_code": _S(), "source": _S(),
}
_DRIVE = {
    "date": _S("ISO date"), "origin": _S(), "destination": _S(),
    "via": _p("array", "Places passed through", items=_S()),
    "means": _S("Means of travel", enum=list(DRIVE_MEANS)), "sequence": _p("integer", "Order within the day"),
    "source": _S(),
}


def _kind_tools(kind, idarg, props):
    spec = RECORD_KINDS[kind]
    label = kind.replace("_", " ")
    mutable = {m: props[m] for m in spec["mutable"] if m in props}
    return {
        f"create_{kind}": ("write", "2A", f"Create a {label} record. Rejects duplicates on the natural key.",
                           _obj(props, spec["required"])),
        f"update_{kind}": ("write", "2A", f"Update mutable fields of a {label}; identity fields ({', '.join(spec['natural_key'])}) cannot change: cancel and record again.",
                           _obj({idarg: _S(f"The {label} record id"), **mutable}, [idarg])),
        f"cancel_{kind}": ("cancel", "2A", f"Cancel a {label} softly. Idempotent. No hard deletes.",
                           _obj({idarg: _S(f"The {label} record id")}, [idarg])),
    }


_NEW = {}
_NEW.update(_kind_tools("lodging", "lodging_id", _LODGING))
_NEW.update(_kind_tools("car_rental", "car_rental_id", _CAR))
_NEW.update(_kind_tools("drive", "drive_id", _DRIVE))
_NEW.update({
    "get_travel_record": ("read", "2A", "Get one lodging, car rental or drive record by id.",
                          _obj({"record_id": _S("A lodging-, car_rental- or drive- record id")}, ["record_id"])),
    "search_travel_records": ("read", "2A", "Search lodging, car rental and drive records. Paginated.",
                              _obj({"kind": _S(enum=_KINDS), **_RANGE, "area_id": _S("Metro id, city:<slug>"),
                                    "status": _S(enum=list(STATUSES)), "limit": _p("integer"), "next_token": _S()})),
    "get_home_places": ("read", "2B", "Get the home places for one residence, or for all residences.",
                        _obj({"residence_uid": _S("Residence period id; omit for all")})),
    "set_home_places": ("write", "2B", "Set the places that count as leaving a residence (airports by code, cities).",
                        _obj({"residence_uid": _S(), "valid_from": _S(), "valid_to": _S(),
                              "places": _p("array", items=_obj({"place_id": _S("type:code, type in apt|stn|city|addr", pattern=PLACE_ID_PATTERN),
                                                                "label": _S(), "radius_km": _p("number")}, ["place_id"]))},
                             ["residence_uid", "places"])),
    "list_trips": ("read", "3A", "List trips: under way, then upcoming, then past (newest first).", _obj(dict(_RANGE))),
    "get_trip": ("read", "3A", "Get one trip, with the rule behind each boundary.",
                 _obj({"trip_key": _S("The id of the trip's first record")}, ["trip_key"])),
    "list_undecided_trips": ("read", "3B", "List trips with an open flag or signal, each with one prepared question and its answers.",
                             _obj({**_RANGE, "limit": _p("integer")})),
    "answer_trip_question": ("write", "3B", "Pass back the chosen answer to a prepared question; shows the correction it writes (dry_run), then writes it.",
                             _obj({"trip_key": _S(), "question_id": _S(), "answer_id": _S(), **_SESSION},
                                  ["trip_key", "question_id", "answer_id"])),
    "record_trip_correction": ("write", "2E", "Record a correction in free form, for a case the prepared answers miss.",
                               _obj({"kind": _S(enum=list(CORRECTION_KINDS)), "date": _S("ISO date; required for returned and left"),
                                     "record_ids": _p("array", items=_S()), "reason": _S(), "question_id": _S(), **_SESSION},
                                    ["kind", "record_ids"])),
    "retire_trip_correction": ("write", "2E", "Retire a correction, reopening what it closed. Never deletes.",
                               _obj({"correction_id": _S(), "reason": _S(), **_SESSION}, ["correction_id"])),
})

NEW_TOOL_NAMES = tuple(_NEW)
TOOL_NAMES = FLIGHT_TOOLS + NEW_TOOL_NAMES
NEW_TOOL_SCHEMAS = {n: v[3] for n, v in _NEW.items()}  # business schemas; dry_run/idempotency_key are added to writes at serve time
NEW_TOOL_DESCRIPTIONS = {n: v[2] for n, v in _NEW.items()}
NEW_WRITE_TOOLS = tuple(n for n, v in _NEW.items() if v[0] in ("write", "cancel"))
NEW_READ_TOOLS = tuple(n for n, v in _NEW.items() if v[0] == "read")
TOOL_LANES = {n: v[1] for n, v in _NEW.items()}  # the lane that implements each handler
LANE_MODULES = {  # lane -> module a lane drops into travel_mcp/ exposing HANDLERS = {tool_name: async handler}
    "2A": "lane_2a_records", "2B": "lane_2b_home", "2E": "lane_2e_corrections",
    "3A": "lane_3a_trips", "3B": "lane_3b_repair",
}
_RO = dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
NEW_TOOL_ANNOTATIONS = {
    n: (_RO if v[0] == "read" else
        dict(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False) if v[0] == "cancel" else
        dict(readOnlyHint=False, destructiveHint=False, idempotentHint=n in ("set_home_places", "retire_trip_correction"),
             openWorldHint=False))
    for n, v in _NEW.items()
}
