"""A-1 (FLY-TSK-028-0A): the contract test.

Pins the record kinds, the 24 tool names and their schemas, the four vocabularies, the settings, the
author stamp with its optional session, and that the five flight tools' schemas equal those at 4e0cc8d
(the commit before this plan). Offline. Tests that serve through lambda_function need io_kit_mcp_edge
and skip without it; CI installs it and is authoritative.
"""
import asyncio
import json
import os
import re
import sys
from pathlib import Path

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
FX = ROOT / "tests" / "fixtures"
sys.path.insert(0, str(D))
import contract as c  # noqa: E402


def _lf():
    pytest.importorskip("io_kit_mcp_edge")
    import lambda_function
    return lambda_function


# ---------------------------------------------------------------- tool names
FLIGHT = ["search_flights", "get_flight", "create_flight", "update_flight", "cancel_flight"]
NEW = [
    "create_lodging", "update_lodging", "cancel_lodging",
    "create_car_rental", "update_car_rental", "cancel_car_rental",
    "create_drive", "update_drive", "cancel_drive",
    "get_travel_record", "search_travel_records",
    "get_home_places", "set_home_places",
    "list_trips", "get_trip",
    "list_undecided_trips", "answer_trip_question", "record_trip_correction", "retire_trip_correction",
]


def test_24_tool_names():
    assert list(c.TOOL_NAMES) == FLIGHT + NEW
    assert len(c.TOOL_NAMES) == 24 and len(set(c.TOOL_NAMES)) == 24
    assert list(c.FLIGHT_TOOLS) == FLIGHT and list(c.NEW_TOOL_NAMES) == NEW


def test_new_tool_schemas_pinned():
    g = json.loads((FX / "contract_tools_1.0.0.json").read_text())
    assert json.loads(json.dumps(c.NEW_TOOL_SCHEMAS)) == g["new_tool_schemas"]
    assert c.NEW_TOOL_DESCRIPTIONS == g["new_tool_descriptions"]
    assert json.loads(json.dumps(c.NEW_TOOL_ANNOTATIONS)) == g["annotations"]
    assert c.TOOL_LANES == g["lanes"]
    for name, schema in c.NEW_TOOL_SCHEMAS.items():
        assert schema["type"] == "object" and set(schema.get("required", [])) <= set(schema["properties"]), name


def test_write_and_read_split_and_lanes():
    assert set(c.NEW_READ_TOOLS) == {"get_travel_record", "search_travel_records", "get_home_places", "list_trips",
                                     "get_trip", "list_undecided_trips"}
    assert len(c.NEW_WRITE_TOOLS) == 13 and set(c.NEW_WRITE_TOOLS) | set(c.NEW_READ_TOOLS) == set(NEW)
    assert set(c.TOOL_LANES.values()) == {"2A", "2B", "2E", "3A", "3B"} == set(c.LANE_MODULES)


def test_session_is_optional_and_author_is_never_input():
    for t in ("answer_trip_question", "record_trip_correction", "retire_trip_correction"):
        s = c.NEW_TOOL_SCHEMAS[t]
        assert "session" in s["properties"] and "session" not in s.get("required", [])
        assert "author" not in s["properties"]


# ---------------------------------------------------------------- record kinds, keys, stamp
def test_record_kinds():
    assert set(c.RECORD_KINDS) == {"lodging", "car_rental", "drive", "home_places", "correction"}
    assert c.RECORD_KINDS["lodging"]["natural_key"] == ["first_night", "name", "city", "state", "country"]
    assert c.RECORD_KINDS["car_rental"]["natural_key"] == ["pickup_date", "pickup_place", "company", "confirmation_code"]
    assert c.RECORD_KINDS["drive"]["natural_key"] == ["date", "origin", "destination", "sequence"]
    assert c.RECORD_KINDS["home_places"]["natural_key"] == ["residence_uid"]
    assert {"city", "state", "country"} <= set(c.RECORD_KINDS["lodging"]["required"])
    assert c.RECORD_KINDS["correction"]["fields"] == [
        "kind", "date", "record_ids", "reason", "author", "session", "question_id", "status",
        "created_at", "retired_at", "retired_by", "first_date", "last_date", "method", "supersedes"]
    for k, spec in c.RECORD_KINDS.items():  # identity fields are immutable
        assert not set(spec["mutable"]) & set(spec["natural_key"]), k
    assert c.STATUSES == ("active", "cancelled") and c.CORRECTION_STATUSES == ("active", "retired")
    assert c.CORRECTION_KINDS == ("returned", "left", "together", "leave_out", "confirmed") and c.DRIVE_MEANS == ("road",)


def test_place_identity_fields():
    assert c.PLACE_TYPES == ("apt", "stn", "city", "addr") and c.PLACE_RESOLUTIONS == ("exact", "geocoded", "unresolved")
    assert re.match(c.PLACE_ID_PATTERN, "apt:SEA") and re.match(c.PLACE_ID_PATTERN, "city:seattle-wa-us")
    assert not re.match(c.PLACE_ID_PATTERN, "SEA") and re.match(c.AREA_ID_PATTERN, "city:seattle-wa-us")
    assert not re.match(c.AREA_ID_PATTERN, "apt:SEA")
    ends = {k: v["place_ends"] for k, v in c.RECORD_KINDS.items()}
    assert ends["car_rental"].keys() == {"pickup", "return"} and ends["drive"].keys() == {"from", "to"}
    for f in ("pickup_place_id", "pickup_area_id", "return_place_id", "return_area_id"):
        assert f in c.RECORD_KINDS["car_rental"]["fields"]
    for f in ("from_place_id", "from_area_id", "to_place_id", "to_area_id"):
        assert f in c.RECORD_KINDS["drive"]["fields"]
    assert c.RECORD_KINDS["home_places"]["places_item_fields"] == ["place_id", "area_id", "label", "radius_km"]


def test_pk_sk_convention():
    t = c.RECORDS_TABLE
    assert (t["hash_key"], t["range_key"], t["sk"]) == ("pk", "sk", "rec")
    assert t["gsi"] == {"area-index": {"hash": "area_id", "range": "date_iso"}}
    k = c.record_keys("lodging", ["2025-06-01", "Hotel X", "Seattle", "WA", "US"])
    assert re.fullmatch(r"lodging#[0-9a-f]{16}", k["pk"]) and k["sk"] == "rec"
    assert k["record_id"] == "lodging-" + k["pk"].split("#")[1]
    assert k == c.record_keys("lodging", [" 2025-06-01", "hotel x", "SEATTLE", "wa", "us "])
    assert k["pk"] != c.record_keys("drive", ["2025-06-01", "Hotel X", "Seattle", "WA", "US"])["pk"]
    for kind, spec in c.RECORD_KINDS.items():
        assert spec["date_iso"] in spec["fields"], kind


def test_author_stamp():
    f = c.AUTHOR_STAMP["fields"]
    assert f == {"author": {"required": True, "type": "string"}, "session": {"required": False, "type": "string"}}
    assert c.AUTHOR_STAMP["input_arg"] == "session"


# ---------------------------------------------------------------- vocabularies
def test_vocabularies():
    assert c.BASES == ("recorded", "derived", "undecided")
    assert set(c.RULES) == {"home_to_home", "one_line", "no_flight_needed", "from_lodging", "forced", "bounded",
                            "connection_or_visit", "a_move"}
    assert {r for r, v in c.RULES.items() if v["derives_boundary"]} == {"from_lodging", "forced", "a_move"}  # `bounded` backs an undecided boundary, never a derived one (FLY-TSK-031-0A)
    assert c.FLAGS == ("start not recorded", "return not recorded", "return not planned yet",
                       "home not recorded for these dates", "is this place home")
    assert c.SIGNALS == ("likely duplicate", "unusually long")
    assert c.TRIP_STATES == ("planned", "upcoming", "under way", "past")
    assert c.TRIP_LIST_ORDER == ("under way", "upcoming", "planned", "past") and set(c.TRIP_LIST_ORDER) == set(c.TRIP_STATES)
    assert set(c.TRIP_STATE_MEANING) == set(c.TRIP_STATES)
    assert c.DISPOSITIONS == ("cancelled", "left_out", "unreadable", "at_home", "a_move", "day_out_and_back")
    assert not hasattr(c, "BASIS")  # the name is BASES
    assert c.TRIP_FIELDS == ["key", "first_date", "first_basis", "last_date", "last_basis", "nights", "places_visited",
                             "legs", "stays", "rentals", "state", "flags", "signals", "residence_period", "corrections", "arc", "confirmation_id"]
    assert c.ARCS == ("detected", "proposed", "confirmed") and c.CONFIRMATION_METHODS == ("examined", "accepted_list")
    assert c.GET_TRIP_EXTRA_FIELDS == ["first_rule", "last_rule"]


def test_questions_one_per_flag_and_signal():
    assert set(c.QUESTIONS) == set(c.FLAGS) | set(c.SIGNALS)
    assert len(set(c.QUESTIONS.values())) == 7
    assert c.QUESTIONS_WITHOUT_TRAVEL_ANSWER == ("q_home_not_recorded",)
    assert (c.QUESTION_ANSWERS_MIN, c.QUESTION_ANSWERS_MAX) == (2, 4)


def test_error_codes():
    assert c.ERROR_CODES == {"duplicate": 409, "invalid": 400, "not_allowed": 403, "missing": 404,
                             "HOME_UNAVAILABLE": 503, "NO_HOME_RECORDED": 422, "not_implemented": 501}
    assert "HOME_UNAUTHORIZED" not in c.ERROR_CODES


# ---------------------------------------------------------------- settings
def test_settings_keys_and_no_instance_values():
    assert set(c.SETTINGS) == {
        "TABLE_NAME", "RECORDS_TABLE_NAME", "SURFACE_NAME", "FINANCE_READ_FUNCTION", "COGNITO_USER_POOL_ID",
        "FINANCE_READ_CREDENTIAL", "FINANCE_READ_CREDENTIAL_SECRET", "COGNITO_CLIENT_ID", "COGNITO_REGION", "COGNITO_ALLOWED_SUBS", "IOGRAPH_ALLOWED_SUBS",
        "TRAVEL_WRITE_GROUPS", "PUBLIC_BASE_URL", "CORS_ORIGINS"}
    assert all(set(v) == {"required", "purpose"} for v in c.SETTINGS.values())
    src = (D / "contract.py").read_text().lower()
    # DOC-05A07958F004 s1: the instance-naming literals that must not appear in the contract
    for literal in ("thepup", "356364570033", "io-travel", "y6bhkpnzn", "6eh4u69l1mqk1m4hfjspeibla8", "us-west-2",
                    "us-east-1", "codeartifact", "devops_agentcli"):
        assert literal not in src, literal


# ---------------------------------------------------------------- the five flight tools are unchanged
def test_flight_tools_equal_4e0cc8d():
    lf = _lf()
    g = json.loads((FX / "flight_tools_4e0cc8d.json").read_text())
    assert g["source_commit"] == "4e0cc8d" and sorted(g["served_tools"]) == sorted(FLIGHT)
    base = {t.name: t.inputSchema for t in lf._base_tools()}
    for name in FLIGHT:
        assert base[name] == g["base_input_schemas"][name], name
    for t in lf._tool_specs():
        if t.name in FLIGHT:
            want = g["served_tools"][t.name]
            assert t.inputSchema == want["inputSchema"], t.name
            assert t.description == want["description"], t.name
            assert (t.annotations.model_dump(exclude_none=True) if t.annotations else None) == want["annotations"], t.name
            assert t.meta == want["meta"], t.name


def test_flight_tool_pin_source_untouched_by_contract():
    # The contract must not define or alter flight schemas: they live in lambda_function.py only.
    assert not set(FLIGHT) & set(c.NEW_TOOL_SCHEMAS)


# ---------------------------------------------------------------- served surface
def test_served_tools_caps_and_manifest():
    lf = _lf()
    specs = lf._tool_specs()
    assert [t.name for t in specs] == FLIGHT + NEW
    by = {t.name: t for t in specs}
    for n in c.NEW_WRITE_TOOLS:
        assert "dry_run" in by[n].inputSchema["properties"] and "idempotency_key" in by[n].inputSchema["properties"], n
    for n in c.NEW_READ_TOOLS:
        assert "dry_run" not in by[n].inputSchema["properties"], n
    caps = lf.build_caps()
    assert [a["name"] for a in caps["actions"]] == FLIGHT + NEW
    pm = lf.build_permission_manifest()
    assert list(pm["tools"]) == FLIGHT + NEW
    assert {n for n, r in pm["tools"].items() if r["access"] == "write"} == set(lf.WRITE_TOOLS)
    assert set(c.NEW_WRITE_TOOLS) <= set(lf.WRITE_TOOLS)


def test_unimplemented_handlers_name_their_lane():
    lf = _lf()
    for name, lane in c.TOOL_LANES.items():
        if lf._TOOL_HANDLERS[name].__module__ != lf.__name__:
            continue  # a lane has landed its handler; its own tests cover it
        out = asyncio.run(lf._TOOL_HANDLERS[name]({}))
        doc = json.loads(out[0].text)
        assert doc["success"] is False and doc["error_envelope"]["code"] == "not_implemented", name
        assert f"lane {lane}" in doc["error_envelope"]["message"] and name in doc["error_envelope"]["message"]
