"""FLY-TSK-030-0A, acceptance A-12: list_trips and get_trip, end to end through the handlers. Offline.

Records live in moto tables (flights + records), home comes from the real home_service over a stubbed
transport, corrections and home places are written through the real lane 2E / 2B handlers, and the real trip
engine does the work. Nothing here reaches AWS, finance or the network.

A-12 clause 1: under way, then upcoming, then past (test_listing_order_*).
A-12 clause 2: get_trip names the rule behind each boundary (test_get_trip_names_*).
A-12 clause 3: a flight, a drive and a stay of one journey are one trip (test_one_journey_*).
Plus: the two HomeError codes surface from the handler.
"""
import asyncio
import json
import os
import sys
import types
from datetime import date
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("TABLE_NAME", "io-travel-flights")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))

try:
    import io_kit_mcp_edge  # noqa: F401
except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
    _stub = types.ModuleType("io_kit_mcp_edge")
    for _n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
        setattr(_stub, _n, type(_n, (Exception,), {}))
    _jw = types.ModuleType("io_kit_mcp_edge.jwks")
    _jw.fetch_https = lambda *a, **k: {}
    sys.modules["io_kit_mcp_edge"], sys.modules["io_kit_mcp_edge.jwks"] = _stub, _jw

import contract  # noqa: E402
import home_service as hs  # noqa: E402
import lambda_function as lf  # noqa: E402
import lane_2a_records  # noqa: E402
import lane_3a_trips as lane  # noqa: E402

FLIGHTS, RECORDS = "io-travel-flights", "test-records"
TODAY = date(2026, 6, 15)
URL, TOKEN = "https://finance.example.test/", "test-bearer"
PERIODS = [{"effective_from": "2020-01-01", "effective_to": None, "end_status": "ongoing", "residence_uid": "res-a",
            "tenure": "rented", "state": "S1", "country": "ZZ"}]


class Finance:
    def __init__(self, periods=PERIODS, status=200, exc=None):
        self.periods, self.status, self.exc, self.windows = periods, status, exc, []

    def __call__(self, url, headers, body, timeout):
        self.windows.append(json.loads(body))
        if self.exc:
            raise self.exc
        inner = {"success": True, "tool": "finance.residence_periods", "result": {"periods": self.periods}}
        env = {"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": json.dumps(inner)}], "isError": False}}
        return self.status, json.dumps(env).encode()


@pytest.fixture
def world(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS)
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-west-2")
        flights = ddb.create_table(TableName=FLIGHTS, BillingMode="PAY_PER_REQUEST",
                                   AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}],
                                   KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}])
        records = ddb.create_table(
            TableName=RECORDS, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk")],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}])
        lf._dynamo_resource = None
        lane_2a_records._resource = None
        import lane_2b_home
        lane_2b_home._resource = None
        monkeypatch.setattr(lf, "TABLE_NAME", FLIGHTS)
        fin = Finance()
        monkeypatch.setattr(lane, "_today", lambda: TODAY)
        monkeypatch.setattr(lane, "_read_home", lambda a, b: hs.read_home(a, b, transport=fin, credential=lambda: TOKEN, endpoint=URL))
        tok = lf._PRINCIPAL.set(dict(lf.INTERNAL_PRINCIPAL))
        w = types.SimpleNamespace(flights=flights, records=records, finance=fin)
        hp = call("set_home_places", {"residence_uid": "res-a", "places": [{"place_id": "apt:SEA", "area_id": "city:seattle"}]})
        assert hp["success"], hp
        yield w
        lf._PRINCIPAL.reset(tok)
        lf._dynamo_resource = None
        lane_2a_records._resource = None
        lane_2b_home._resource = None


def call(tool, args):
    return json.loads(asyncio.run(lf.call_tool(tool, args))[0].text)


def code(out):
    assert out["success"] is False
    return out["error_envelope"]["code"]


def flight(w, fid, day, o, d, status="active"):
    w.flights.put_item(Item={"flight_id": fid, "date_iso": day, "origin": o, "dest": d, "status": status,
                             "origin_place_id": f"apt:{o}", "dest_place_id": f"apt:{d}"})


def rec(w, kind, rid, **fields):
    w.records.put_item(Item={"pk": f"{kind}#{rid}", "sk": "rec", "kind": kind, "record_id": f"{kind}-{rid}", "status": "active", **fields})
    return f"{kind}-{rid}"


def round_trip(w, tag, out_day, back_day, city="JFK"):
    flight(w, f"{tag}-out", out_day, "SEA", city)
    flight(w, f"{tag}-back", back_day, city, "SEA")


def trips(out):
    assert out["success"], out
    return out["result"]["trips"]


# ------------------------------------------------------------------ clause 1: the order
def test_listing_order_under_way_then_upcoming_then_past(world):
    round_trip(world, "old", "2026-03-01", "2026-03-04")
    round_trip(world, "newer", "2026-05-01", "2026-05-04", "LAX")
    round_trip(world, "now", "2026-06-14", "2026-06-18", "BOS")          # today is 2026-06-15
    round_trip(world, "soon", "2026-07-10", "2026-07-14", "ATL")
    round_trip(world, "later", "2026-09-01", "2026-09-05", "DEN")
    out = call("list_trips", {})
    ts = trips(out)
    assert [t["key"] for t in ts] == ["now-out", "soon-out", "later-out", "newer-out", "old-out"]
    assert [t["state"] for t in ts] == ["under way", "upcoming", "upcoming", "past", "past"]
    assert out["result"]["today"] == "2026-06-15"


def test_listing_order_is_the_engines_not_our_own(world, monkeypatch):
    seen = []
    real = lane.trip_engine.order_trips_for_listing
    monkeypatch.setattr(lane.trip_engine, "order_trips_for_listing", lambda t: seen.append(len(t)) or real(t))
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    call("list_trips", {})
    assert seen == [1]


def test_planned_trip_with_no_return_sorts_after_upcoming_before_past(world):
    round_trip(world, "past", "2026-03-01", "2026-03-04")
    flight(world, "open-out", "2026-07-01", "SEA", "ORD")               # no way back recorded: return undecided
    round_trip(world, "soon", "2026-08-01", "2026-08-05", "ATL")
    ts = trips(call("list_trips", {}))
    assert [(t["key"], t["state"]) for t in ts] == [("soon-out", "upcoming"), ("open-out", "planned"), ("past-out", "past")]


def test_date_range_filters_after_joining(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    round_trip(world, "b", "2026-05-01", "2026-05-04", "LAX")
    ts = trips(call("list_trips", {"date_from": "2026-04-01", "date_to": "2026-05-31"}))
    assert [t["key"] for t in ts] == ["b-out"]
    assert code(call("list_trips", {"date_from": "nope"})) == "invalid"


def test_cancelled_records_pass_through_and_are_excluded_with_a_disposition(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    flight(world, "x", "2026-04-01", "SEA", "MIA", status="cancelled")
    out = call("list_trips", {})
    assert [t["key"] for t in trips(out)] == ["a-out"]
    assert [(e["record_id"], e["disposition"]) for e in out["result"]["excluded"]] == [("x", "cancelled")]


# ------------------------------------------------------------------ clause 2: the rule behind each boundary
def test_get_trip_names_a_recorded_boundary_basis(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    r = call("get_trip", {"trip_key": "a-out"})["result"]
    assert r["key"] == "a-out"
    for side in ("first", "last"):
        b = r["boundaries"][side]
        assert b["basis"] in contract.BASES and b["basis"] == "recorded" and b["rule"] is None
    assert r["boundaries"]["first"]["date"] == "2026-03-01" and r["boundaries"]["last"]["date"] == "2026-03-04"
    assert r["first_rule"] is None and r["last_rule"] is None


def test_get_trip_names_the_deriving_rule_and_its_text(world):
    # a stay with no recorded way back: the end is taken from the last check-out
    flight(world, "f-out", "2026-03-01", "SEA", "JFK")
    rec(world, "lodging", "h1", first_night="2026-03-01", last_night="2026-03-05", place_id="city:new-york",
        area_id="city:new-york", city="New York", state="NY", country="US")
    r = call("get_trip", {"trip_key": "f-out"})
    assert r["success"], r
    last = r["result"]["boundaries"]["last"]
    assert last["basis"] == "derived" and last["rule"] == "from_lodging" == r["result"]["last_rule"]
    assert last["rule_text"] == contract.RULES["from_lodging"]["text"]


def test_get_trip_boundary_set_by_a_correction_reads_recorded_with_the_correction_listed(world):
    flight(world, "f-out", "2026-03-01", "SEA", "JFK")
    rec(world, "lodging", "h1", first_night="2026-03-01", last_night="2026-03-05", place_id="city:new-york",
        area_id="city:new-york", city="New York", state="NY", country="US")
    made = call("record_trip_correction", {"kind": "returned", "date": "2026-03-06", "record_ids": ["f-out"], "reason": "stayed on"})
    assert made["success"], made
    t = call("get_trip", {"trip_key": "f-out"})["result"]
    last = t["boundaries"]["last"]
    assert last["basis"] == "recorded" and last["date"] == "2026-03-06"
    assert [(c["kind"], c["date"], c["record_ids"]) for c in last["corrections"]] == [("returned", "2026-03-06", ["f-out"])]
    assert last["corrections"][0]["author"] == made["result"]["author"]
    assert t["boundaries"]["first"]["corrections"] == []


def test_get_trip_unknown_key_and_missing_key(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    assert code(call("get_trip", {"trip_key": "nope"})) == "missing"
    assert code(call("get_trip", {})) == "invalid"


# ------------------------------------------------------------------ clause 3: one journey, three kinds, one trip
def test_one_journey_a_flight_a_drive_and_a_stay_are_one_trip(world):
    flight(world, "j-out", "2026-05-01", "SEA", "LAX")
    rec(world, "lodging", "s1", first_night="2026-05-01", last_night="2026-05-03", place_id="city:los-angeles",
        area_id="city:los-angeles", city="Los Angeles", state="CA", country="US")
    rec(world, "drive", "d1", date="2026-05-02", sequence=1, from_place_id="city:los-angeles", from_area_id="city:los-angeles",
        to_place_id="city:santa-barbara", to_area_id="city:santa-barbara", area_id="city:los-angeles")
    flight(world, "j-back", "2026-05-04", "LAX", "SEA")
    out = call("list_trips", {})
    ts = trips(out)
    assert len(ts) == 1 and out["result"]["excluded"] == []
    t = ts[0]
    assert sorted(t["record_ids"]) == sorted(["j-out", "j-back", "lodging-s1", "drive-d1"])
    assert t["key"] == "j-out" and (t["first_date"], t["last_date"]) == ("2026-05-01", "2026-05-04")
    assert len(t["legs"]) >= 3 and len(t["stays"]) == 1
    got = call("get_trip", {"trip_key": "j-out"})["result"]
    assert sorted(got["record_ids"]) == sorted(t["record_ids"])


def test_one_journey_car_rental_joins_too_and_separate_journeys_stay_separate(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04", "LAX")
    rec(world, "car_rental", "c1", pickup_date="2026-03-01", dropoff_date="2026-03-04", pickup_place_id="apt:LAX",
        pickup_area_id="city:los-angeles", area_id="city:los-angeles", company="X")
    round_trip(world, "b", "2026-06-20", "2026-06-24", "BOS")
    ts = trips(call("list_trips", {}))
    assert len(ts) == 2
    by = {t["key"]: t for t in ts}
    assert "car_rental-c1" in by["a-out"]["record_ids"] and "car_rental-c1" not in by["b-out"]["record_ids"]


# ------------------------------------------------------------------ the two HomeError codes
def test_no_home_recorded_surfaces_with_its_code_from_an_empty_answer(world):
    world.finance.periods = []
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    out = call("list_trips", {})
    assert code(out) == "NO_HOME_RECORDED"


def test_no_home_recorded_surfaces_when_the_engine_refuses(world, monkeypatch):
    class Empty:
        window, periods, uncovered, flags = (), (), (), ()
    monkeypatch.setattr(lane, "_read_home", lambda a, b: Empty())
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    assert code(call("get_trip", {"trip_key": "a-out"})) == "NO_HOME_RECORDED"


@pytest.mark.parametrize("tool,args", [("list_trips", {}), ("get_trip", {"trip_key": "a-out"})])
def test_home_unavailable_surfaces_retryable_and_leaks_nothing(world, tool, args):
    world.finance.exc = OSError("connection refused: secret-host")
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    out = call(tool, args)
    assert code(out) == "HOME_UNAVAILABLE" and out["error_envelope"]["retryable"] is True
    assert "secret-host" not in json.dumps(out) and TOKEN not in json.dumps(out)


def test_home_is_asked_for_the_records_window_and_no_home_argument(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    call("list_trips", {})
    w = world.finance.windows[0]
    assert "2026-03-01" in json.dumps(w) and "2026-06-15" in json.dumps(w)


def test_registered_through_the_dispatcher_not_a_stub(world):
    assert set(lane.HANDLERS) == {"list_trips", "get_trip"} and contract.LANE_MODULES["3A"] == "lane_3a_trips"
    out = call("list_trips", {})
    assert out["success"] is True and out["result"]["trips"] == []


# ------------------------------------------------------------------ FLY-TSK-045 (A-37): arc on every trip read
def test_list_trips_and_get_trip_carry_arc_and_a_confirmation_follows_the_records(world):
    round_trip(world, "a", "2026-03-01", "2026-03-04")
    flight(world, "open", "2026-05-01", "SEA", "LAX")                    # no way back: a flag, so detected
    before = trips(call("list_trips", {}))
    by = {t["key"]: t for t in before}
    assert (by["a-out"]["arc"], by["a-out"]["confirmation_id"]) == ("proposed", None) and by["open"]["arc"] == "detected"
    assert by["a-out"]["state"] == "past"
    conf = call("record_trip_correction", {"kind": "confirmed", "record_ids": ["a-out", "a-back"], "first_date": "2026-03-01",
                                           "last_date": "2026-03-04", "method": "examined"})
    assert conf["success"], conf
    cid = conf["result"]["correction_id"]
    after = call("list_trips", {})
    assert [{k: v for k, v in t.items() if k not in ("arc", "confirmation_id")} for t in trips(after)] == \
        [{k: v for k, v in t.items() if k not in ("arc", "confirmation_id")} for t in before]
    assert after["result"]["unapplied_corrections"] == []
    one = call("get_trip", {"trip_key": "a-out"})["result"]
    assert (one["arc"], one["confirmation_id"], one["state"]) == ("confirmed", cid, "past")
    assert call("get_trip", {"trip_key": "a-out"})["result"] == one          # two reads agree
    leave = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a-back"]})
    assert leave["success"]
    now = call("list_trips", {})["result"]
    assert [u["correction_id"] for u in now["unapplied_corrections"]] == [cid]
