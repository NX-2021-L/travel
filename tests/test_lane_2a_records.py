"""FLY-TSK-029-0A, acceptance A-6: lodging, car rental and drive records, offline (moto), never a live table.

A-6: each kind is created after a dry run, refused as a duplicate on the same natural key, cancelled softly,
and a stay is refused when city, state or country is missing.
"""
import asyncio
import json
import os
import sys
import types
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("TABLE_NAME", "io-travel-flights")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"))

try:
    import io_kit_mcp_edge  # noqa: F401
except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
    _stub = types.ModuleType("io_kit_mcp_edge")
    for _n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
        setattr(_stub, _n, type(_n, (Exception,), {}))
    _jw = types.ModuleType("io_kit_mcp_edge.jwks")
    _jw.fetch_https = lambda *a, **k: {}
    sys.modules["io_kit_mcp_edge"], sys.modules["io_kit_mcp_edge.jwks"] = _stub, _jw

import lambda_function as lf  # noqa: E402
import lane_2a_records as lane  # noqa: E402

RECORDS = "test-records"
STAY = {"name": "Hotel Max", "city": "Seattle", "state": "WA", "country": "US", "first_night": "2026-06-01", "last_night": "2026-06-04"}
CAR = {"company": "Hertz", "pickup_place": "SEA airport", "pickup_date": "2026-06-01", "dropoff_place": "SEA airport",
       "dropoff_date": "2026-06-04", "confirmation_code": "H123"}
DRIVE = {"date": "2026-06-02", "origin": "Seattle", "destination": "Portland", "sequence": 1}
KIND_ARGS = {"lodging": STAY, "car_rental": CAR, "drive": DRIVE}
ID_ARG = {"lodging": "lodging_id", "car_rental": "car_rental_id", "drive": "drive_id"}
MUTATE = {"lodging": {"last_night": "2026-06-05"}, "car_rental": {"dropoff_date": "2026-06-05"}, "drive": {"means": "road", "via": ["Tacoma"]}}


@pytest.fixture
def table(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS)
    with mock_aws():
        lane._resource = None
        t = boto3.resource("dynamodb", region_name="us-west-2").create_table(
            TableName=RECORDS, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "area_id", "date_iso")],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[{"IndexName": "area-index", "Projection": {"ProjectionType": "ALL"},
                                     "KeySchema": [{"AttributeName": "area_id", "KeyType": "HASH"}, {"AttributeName": "date_iso", "KeyType": "RANGE"}]}])
        tok = lf._PRINCIPAL.set(dict(lf.INTERNAL_PRINCIPAL))
        yield t
        lf._PRINCIPAL.reset(tok)
        lane._resource = None


def call(tool, args):
    return json.loads(asyncio.run(lf.call_tool(tool, args))[0].text)


def code(out):
    assert out["success"] is False
    return out["error_envelope"]["code"]


def test_all_eleven_handlers_replace_the_stub():
    assert set(lane.HANDLERS) == {t for t, l in lf.contract.TOOL_LANES.items() if l == "2A"} and len(lane.HANDLERS) == 11
    for name, h in lane.HANDLERS.items():
        assert lf._TOOL_HANDLERS[name] is h


# --- A-6.1 created after a dry run
@pytest.mark.parametrize("kind", lane.KINDS)
def test_created_after_a_dry_run(table, kind):
    dry = call(f"create_{kind}", {**KIND_ARGS[kind], "dry_run": True})
    rc = dry["result"]["resolved_call"]
    assert rc["source"] == "server-dry-run" and rc["sideEffects"]["kinds"] == ["dynamodb:PutItem"]
    assert table.scan()["Count"] == 0  # a dry run changes nothing
    out = call(f"create_{kind}", KIND_ARGS[kind])
    rec = out["result"]
    assert rec["kind"] == kind and rec["status"] == "active" and rec["sk"] == "rec"
    assert rec["pk"] == rc["underlying"][0]["body"]["pk"] and rec["record_id"] == f"{kind}-{rec['pk'].split('#')[1]}"
    assert rec["author"] == "internal-key" and rec["date_iso"] and rec["area_id"].startswith("city:")
    assert rec["place_resolution"] in ("exact", "geocoded", "unresolved")
    assert table.scan()["Count"] == 1


# --- A-6.2 refused as duplicate on the same natural key
@pytest.mark.parametrize("kind", lane.KINDS)
def test_duplicate_on_same_natural_key_refused(table, kind):
    call(f"create_{kind}", KIND_ARGS[kind])
    again = call(f"create_{kind}", {**KIND_ARGS[kind], "source": "other"})  # non-key field differs
    assert code(again) == "duplicate"
    assert call(f"create_{kind}", {**KIND_ARGS[kind], "dry_run": True})["result"]["resolved_call"]["warnings"]
    assert table.scan()["Count"] == 1


def test_duplicate_key_is_case_folded_and_different_key_is_not_a_duplicate(table):
    call("create_lodging", STAY)
    assert code(call("create_lodging", {**STAY, "name": "HOTEL MAX", "city": "seattle"})) == "duplicate"
    assert call("create_lodging", {**STAY, "first_night": "2026-07-01", "last_night": "2026-07-02"})["success"]


def test_duplicate_enforced_by_condition_not_read_first(table):
    rec = call("create_drive", DRIVE)["result"]
    with pytest.raises(table.meta.client.exceptions.ConditionalCheckFailedException):
        table.put_item(Item=rec, ConditionExpression="attribute_not_exists(pk)")


# --- A-6.3 cancelled softly
@pytest.mark.parametrize("kind", lane.KINDS)
def test_cancelled_softly(table, kind):
    rid = call(f"create_{kind}", KIND_ARGS[kind])["result"]["record_id"]
    arg = {ID_ARG[kind]: rid}
    dry = call(f"cancel_{kind}", {**arg, "dry_run": True})
    assert dry["result"]["resolved_call"]["diff"][0]["after"] == "cancelled"
    assert table.scan()["Items"][0]["status"] == "active"
    out = call(f"cancel_{kind}", arg)["result"]
    assert out["status"] == "cancelled" and out["cancelled_at"]
    assert table.scan()["Count"] == 1  # the item is kept
    assert call(f"cancel_{kind}", arg)["result"]["status"] == "cancelled"  # idempotent
    assert code(call(f"update_{kind}", {**arg, **MUTATE[kind]})) == "not_allowed"
    assert code(call(f"create_{kind}", KIND_ARGS[kind])) == "duplicate"  # cancelled still holds the key
    assert code(call(f"cancel_{kind}", {ID_ARG[kind]: f"{kind}-0000000000000000"})) == "missing"


def test_module_never_deletes():
    src = Path(lane.__file__).read_text()
    assert "delete_item" not in src and "batch_writer" not in src


# --- A-6.4 a stay needs city, state and country
@pytest.mark.parametrize("field", ["city", "state", "country"])
@pytest.mark.parametrize("bad", ["absent", "empty", "blank"])
def test_stay_refused_without_city_state_country(table, field, bad):
    args = {k: v for k, v in STAY.items() if k != field}
    if bad == "empty":
        args[field] = ""
    elif bad == "blank":
        args[field] = "   "
    for dry in (True, False):
        out = call("create_lodging", {**args, "dry_run": dry})
        assert code(out) == "invalid" and field in out["error"]
    assert table.scan()["Count"] == 0


def test_rules_are_not_over_applied_to_drives_and_rentals(table):
    # no city/state/country on either kind, and both are accepted
    assert call("create_drive", DRIVE)["success"]
    assert call("create_car_rental", CAR)["success"]


def test_other_validation(table):
    assert code(call("create_lodging", {**STAY, "first_night": "June"})) == "invalid"
    assert code(call("create_lodging", {**STAY, "last_night": "2026-05-01"})) == "invalid"
    assert code(call("create_lodging", {**STAY, "bogus": 1})) == "invalid"
    assert code(call("create_lodging", {**STAY, "place_id": "xyz:1", "area_id": "city:seattle"})) == "invalid"
    assert code(call("create_drive", {**DRIVE, "means": "rail"})) == "invalid"
    assert code(call("create_car_rental", {k: v for k, v in CAR.items() if k != "company"})) == "invalid"


def test_place_identity_stored(table):
    d = call("create_lodging", STAY)["result"]
    assert d["area_id"] == "city:seattle-wa-us" and d["place_id"] == "addr:seattle-wa-us" and d["place_resolution"] == "unresolved"
    e = call("create_lodging", {**STAY, "name": "B", "place_id": "city:seattle-wa-us", "area_id": "city:seattle-wa-us"})["result"]
    assert e["place_resolution"] == "exact"
    r = call("create_car_rental", {**CAR, "pickup_place_id": "apt:SEA", "pickup_area_id": "city:seattle-wa-us",
                                   "return_place_id": "apt:PDX", "return_area_id": "city:portland-or-us"})["result"]
    assert r["area_id"] == "city:seattle-wa-us" and r["return_area_id"] == "city:portland-or-us"
    assert table.query(IndexName="area-index", KeyConditionExpression=Key("area_id").eq("city:seattle-wa-us"))["Count"] == 3


# --- updates
@pytest.mark.parametrize("kind", lane.KINDS)
def test_update_mutable_only(table, kind):
    rid = call(f"create_{kind}", KIND_ARGS[kind])["result"]["record_id"]
    arg = {ID_ARG[kind]: rid}
    dry = call(f"update_{kind}", {**arg, **MUTATE[kind], "dry_run": True})
    assert dry["result"]["resolved_call"]["diff"]
    out = call(f"update_{kind}", {**arg, **MUTATE[kind]})["result"]
    for k, v in MUTATE[kind].items():
        assert out[k] == v
    ident = {"lodging": "name", "car_rental": "company", "drive": "origin"}[kind]
    assert code(call(f"update_{kind}", {**arg, ident: "x"})) == "invalid"
    assert code(call(f"update_{kind}", arg)) == "invalid"
    assert code(call(f"update_{kind}", {ID_ARG[kind]: f"{kind}-0000000000000000", **MUTATE[kind]})) == "missing"
    other = [k for k in lane.KINDS if k != kind][0]
    assert code(call(f"update_{kind}", {ID_ARG[kind]: f"{other}-{rid.split('-')[-1]}", **MUTATE[kind]})) == "invalid"


# --- reads
def test_get_travel_record(table):
    rid = call("create_drive", DRIVE)["result"]["record_id"]
    assert call("get_travel_record", {"record_id": rid})["result"]["record_id"] == rid
    assert code(call("get_travel_record", {"record_id": "drive-0000000000000000"})) == "missing"
    assert code(call("get_travel_record", {"record_id": "flight-abc"})) == "invalid"


def test_search_travel_records(table):
    call("create_lodging", STAY)
    call("create_car_rental", CAR)
    d = call("create_drive", DRIVE)["result"]
    call("create_drive", {**DRIVE, "date": "2026-08-01"})
    table.put_item(Item={"pk": "correction#x", "sk": "rec", "kind": "correction", "status": "active", "date_iso": "2026-06-02"})
    s = lambda **a: call("search_travel_records", a)["result"]
    assert s()["count"] == 4  # lane 2A kinds only; the correction row is not listed
    assert s(kind="drive")["count"] == 2
    assert [r["date_iso"] for r in s(kind="drive")["records"]] == ["2026-06-02", "2026-08-01"]
    assert s(date_from="2026-06-02", date_to="2026-06-30")["count"] == 1
    assert s(area_id="city:seattle-wa-us")["count"] == 1
    call("cancel_drive", {"drive_id": d["record_id"]})
    assert s(kind="drive", status="cancelled")["count"] == 1 and s(kind="drive", status="active")["count"] == 1
    page1 = s(limit=3)
    assert page1["count"] == 3 and page1["next_token"]
    page2 = s(limit=3, next_token=page1["next_token"])
    assert page2["count"] == 1 and "next_token" not in page2
    assert code(call("search_travel_records", {"kind": "flight"})) == "invalid"
    assert code(call("search_travel_records", {"next_token": "zz"})) == "invalid"


# --- runtime caveat: no table yet
def test_unset_setting_is_a_structured_error(monkeypatch):
    monkeypatch.delenv("RECORDS_TABLE_NAME", raising=False)
    tok = lf._PRINCIPAL.set(dict(lf.INTERNAL_PRINCIPAL))
    try:
        for tool, args in (("create_lodging", STAY), ("search_travel_records", {}), ("get_travel_record", {"record_id": "drive-0000000000000000"})):
            out = call(tool, args)
            assert "RECORDS_TABLE_NAME" in out["error"] and out["error_envelope"]["code"] == "internal_error"
    finally:
        lf._PRINCIPAL.reset(tok)


def test_missing_table_is_a_structured_error(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", "no-such-table")
    with mock_aws():
        lane._resource = None
        tok = lf._PRINCIPAL.set(dict(lf.INTERNAL_PRINCIPAL))
        try:
            out = call("create_drive", DRIVE)
            assert "RECORDS_TABLE_NAME" in out["error"] and "unreachable" in out["error"]
        finally:
            lf._PRINCIPAL.reset(tok)
            lane._resource = None
