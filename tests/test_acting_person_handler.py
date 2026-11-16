"""FLY-TSK-032-0B (lane 3D): A-29, A-30, A-31, A-32 through the REAL lambda_handler. Offline (moto).

Internal mode: the role is admitted, the event's acting_principal is stamped as author / retired_by through the existing
author stamp, a write with no acting person answers `forbidden` and writes nothing (dry run included), and
TRAVEL_WRITE_GROUPS is not applied. Public mode: the two keys have no effect, at the top level or in a forged io_kit_mcp.
"""
import json
import logging
import os
import sys
import types
from pathlib import Path

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("TABLE_NAME", "io-travel-flights")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"))
pytest.importorskip("mcp")
boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")
try:
    import io_kit_mcp_edge  # noqa: F401
except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
    _stub = types.ModuleType("io_kit_mcp_edge")
    for _n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
        setattr(_stub, _n, type(_n, (Exception,), {}))
    _jw = types.ModuleType("io_kit_mcp_edge.jwks")
    _jw.fetch_https = lambda *a, **k: {}
    sys.modules.setdefault("io_kit_mcp_edge", _stub)
    sys.modules.setdefault("io_kit_mcp_edge.jwks", _jw)

import contract  # noqa: E402
import lambda_function as lf  # noqa: E402
import lane_2a_records  # noqa: E402
import lane_2b_home  # noqa: E402

ROLE = "arn:aws:iam::000000000000:role/test-nana-caller"
DEPLOY = "arn:aws:iam::000000000000:role/test-deployer"
RECORDS = "test-records"
STAY = {"name": "Hotel Max", "city": "Seattle", "state": "WA", "country": "US", "first_night": "2026-06-01", "last_night": "2026-06-04"}
CAR = {"company": "Hertz", "pickup_place": "SEA airport", "pickup_date": "2026-06-01", "dropoff_place": "SEA airport",
       "dropoff_date": "2026-06-04", "confirmation_code": "H123"}
DRIVE = {"date": "2026-06-02", "origin": "Seattle", "destination": "Portland", "sequence": 1}
SEA = {"place_id": "apt:SEA", "area_id": "city:seattle-wa-us", "label": "Sea-Tac", "lat": 47.45, "lon": -122.31, "radius_km": 60}
CORR = {"kind": "leave_out", "record_ids": ["fl-9"], "reason": "not my travel"}
WHO = {"acting_principal": "nana", "session": "sess-event"}


@pytest.fixture
def internal(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS)
    monkeypatch.setattr(lf, "DOOR_MODE", "internal")
    monkeypatch.setattr(lf, "INTERNAL_INVOKER_ROLE", ROLE)
    monkeypatch.setattr(lf, "INTERNAL_DEPLOY_ROLE", DEPLOY)
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", ["some-group"])  # the role has no groups: applied, it would refuse every write
    with moto.mock_aws():
        for mod in (lane_2a_records, lane_2b_home):
            mod._resource = None
        db = boto3.resource("dynamodb", region_name="us-west-2")
        db.create_table(
            TableName=RECORDS, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in ("pk", "sk", "area_id", "date_iso")],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[{"IndexName": "area-index", "Projection": {"ProjectionType": "ALL"},
                                     "KeySchema": [{"AttributeName": "area_id", "KeyType": "HASH"}, {"AttributeName": "date_iso", "KeyType": "RANGE"}]}])
        db.create_table(TableName="io-travel-flights", BillingMode="PAY_PER_REQUEST",
                        KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
                        AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}])
        lf._dynamo_resource = None
        yield db.Table(RECORDS)
        lf._dynamo_resource = None


def direct(name, arguments, **keys):
    return {"invoker_role": ROLE, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                          "params": {"name": name, "arguments": arguments}}, **keys}


def body(resp):
    assert resp["statusCode"] == 200, resp
    return json.loads(json.loads(resp["body"])["result"]["content"][0]["text"])


def run(event):
    return body(lf.lambda_handler(event, None))


def rows(t):
    return t.scan()["Items"]


def flights():
    return lf._get_table().scan()["Items"]


# ---------------------------------------------------------------- A-29: the stamp
@pytest.mark.parametrize("tool,args", [("create_lodging", STAY), ("create_car_rental", CAR), ("create_drive", DRIVE)])
def test_creates_stamp_the_person_and_event_session_never_the_role(internal, tool, args):
    out = run(direct(tool, args, **WHO))
    assert out["success"] is True, out
    (item,) = rows(internal)
    assert item["author"] == "nana" and item["session"] == "sess-event" and ROLE not in json.dumps(item, default=str)


def test_session_argument_wins_over_the_event_session(internal):
    run(direct("create_lodging", {**STAY, "session": "sess-arg"}, **WHO))
    assert rows(internal)[0]["session"] == "sess-arg"


def test_event_without_session_stores_no_session_attribute(internal):
    run(direct("create_lodging", STAY, acting_principal="nana"))
    item = rows(internal)[0]
    assert item["author"] == "nana" and "session" not in item


def test_set_home_places_stamps_the_person(internal):
    out = run(direct("set_home_places", {"residence_uid": "res-1", "places": [SEA], "valid_from": "2020-01-01"}, **WHO))
    assert out["success"] is True, out
    (item,) = rows(internal)
    assert item["author"] == "nana" and item["session"] == "sess-event"


def test_corrections_stamp_author_and_retired_by_and_sessions(internal):
    boto3.resource("dynamodb", region_name="us-west-2").Table("io-travel-flights").put_item(Item={"flight_id": "fl-9"})  # a correction names a record that exists
    rec = run(direct("record_trip_correction", CORR, **WHO))
    assert rec["success"] is True, rec
    (item,) = rows(internal)
    assert item["author"] == "nana" and item["session"] == "sess-event"
    cid = rec["result"]["correction_id"]
    ret = run(direct("retire_trip_correction", {"correction_id": cid}, acting_principal="io", session="sess-two"))
    assert ret["success"] is True, ret
    (item,) = rows(internal)
    assert item["retired_by"] == "io" and item["retired_session"] == "sess-two" and item["author"] == "nana"
    assert ROLE not in json.dumps(item, default=str)


def test_answer_trip_question_stamps_through_record_trip_correction():
    import lane_3b_repair
    src = Path(lane_3b_repair.__file__).read_text()
    assert 'corrections_lane.HANDLERS["record_trip_correction"]' in src  # no stamping path of its own
    answer = src.split("async def answer_trip_question")[1].split("HANDLERS = ")[0]
    assert '"author"' not in answer and "_PRINCIPAL" not in answer


def test_no_tool_gains_a_property(internal):
    resp = lf.lambda_handler({"invoker_role": ROLE, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}}, None)
    tools = json.loads(resp["body"])["result"]["tools"]
    assert len(tools) == 24
    assert not any("acting_principal" in json.dumps(t["inputSchema"]) for t in tools)


# ---------------------------------------------------------------- A-30: refusal
ALL_WRITES = sorted(lf.WRITE_TOOLS)
NO_PERSON = [{}, {"acting_principal": ""}, {"acting_principal": "  "}, {"acting_principal": 7}, {"acting_principal": ["nana"]},
             {"acting_principal": "nana@example.com"}, {"acting_principal": "a b"}, {"acting_principal": "x" * 129},
             {"acting_principal": ROLE}, {"acting_principal": DEPLOY}, {"session": "only-a-session"}]


def _filled(schema):
    """Arguments that pass the tool's input schema (required properties only), so the call reaches authorisation."""
    def one(p):
        t = p.get("type")
        t = t[0] if isinstance(t, list) else t
        if p.get("enum"):
            return p["enum"][0]
        if p.get("pattern", "").startswith("^(apt|stn|city|addr):"):
            return "apt:SEA"
        if t == "array":
            return [one(p.get("items") or {})] * max(1, p.get("minItems", 1))
        if t == "object":
            return _filled(p)
        return 1 if t in ("integer", "number") else False if t == "boolean" else "x"
    return {name: one(schema["properties"][name]) for name in schema.get("required") or []}


def test_sixteen_write_tools():
    assert len(ALL_WRITES) == 16


@pytest.mark.parametrize("keys", NO_PERSON)
@pytest.mark.parametrize("dry", [False, True])
def test_every_write_is_forbidden_and_writes_nothing_without_an_acting_person(internal, keys, dry):
    specs = {t.name: t.inputSchema for t in lf._tool_specs()}
    for tool in ALL_WRITES:
        args = {**_filled(specs[tool]), **({"dry_run": True} if dry else {})}
        out = run(direct(tool, args, **keys))
        assert out["success"] is False and out["error_envelope"]["code"] == "forbidden", (tool, out)
    assert rows(internal) == [] and flights() == []


def test_refusal_holds_straight_after_an_event_that_named_a_person(internal):
    assert run(direct("create_lodging", STAY, **WHO))["success"] is True
    out = run(direct("create_drive", DRIVE))
    assert out["success"] is False and "forbidden" in json.dumps(out)
    assert len(rows(internal)) == 1 and lf._PRINCIPAL.get() is None and lf._ACTING_SESSION.get() == ""
    assert run(direct("create_car_rental", CAR, acting_principal=ROLE))["success"] is False
    assert len(rows(internal)) == 1


def test_a_session_never_leaks_into_the_next_event(internal):
    run(direct("create_lodging", STAY, **WHO))
    run(direct("create_drive", DRIVE, acting_principal="io"))
    drive = [r for r in rows(internal) if r["kind"] == "drive"][0]
    assert drive["author"] == "io" and "session" not in drive


def test_reads_tools_list_initialize_and_health_are_served_without_a_person(internal):
    for name, args in (("search_flights", {}), ("list_undecided_trips", {}), ("get_home_places", {"residence_uid": "r"})):
        resp = lf.lambda_handler(direct(name, args), None)
        assert resp["statusCode"] == 200 and "forbidden" not in resp["body"], (name, resp)
    init = {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}
    for method, params in (("tools/list", {}), ("initialize", init)):
        resp = lf.lambda_handler({"invoker_role": ROLE, "mcp": {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}}, None)
        assert resp["statusCode"] == 200, (method, resp)
    for r in (ROLE, DEPLOY):
        out = lf.lambda_handler({"invoker_role": r, "mcp": {"jsonrpc": "2.0", "id": 3, "method": "ping"}}, None)
        assert out["result"]["status"] == "ok"


def test_non_string_invoker_role_is_refused_through_the_handler(internal):
    for claimed in (["x"], {"a": 1}, 7, True):
        assert lf.lambda_handler({"invoker_role": claimed, "mcp": {"method": "tools/list"}}, None)["statusCode"] == 403


# ---------------------------------------------------------------- A-31: groups, audit
def test_write_groups_are_not_applied_behind_the_internal_door(internal):
    assert lf.TRAVEL_WRITE_GROUPS == ["some-group"]
    assert run(direct("create_lodging", STAY, acting_principal="nana"))["success"] is True


def test_authorize_tests_only_presence_of_the_person():
    base = {"kind": "internal-role", "id": "x", "groups": [], "allowed": True, "role": ROLE}
    assert lf._authorize({**base, "acting_principal": "anything-at-all"}, "create_lodging") is None
    assert lf._authorize({**base, "acting_principal": ""}, "create_lodging")
    assert lf._authorize({**base, "acting_principal": ""}, "search_flights") is None


def audit_lines(caplog):
    return [json.loads(r.getMessage()) for r in caplog.records if r.getMessage().startswith('{"audit"')]


def test_the_door_audit_line_names_role_person_and_session_for_every_tool(internal, caplog):
    with caplog.at_level(logging.INFO):
        run(direct("create_lodging", STAY, **WHO))
        run(direct("update_flight", {"flight_id": "f"}, **WHO))  # stores no person: the audit line is the record
        run(direct("search_flights", {}, **WHO))
        run(direct("create_drive", DRIVE))  # refused: still audited
    by = {}
    for line in audit_lines(caplog):
        by.setdefault(line["tool"], []).append(line)
    for tool in ("create_lodging", "update_flight", "search_flights"):
        (line,) = by[tool]
        assert line["invoker_role"] == ROLE and line["acting_principal"] == "nana" and line["session"] == "sess-event"
    (refused,) = by["create_drive"]
    assert refused["allowed"] is False and refused["invoker_role"] == ROLE and refused["acting_principal"] is None


# ---------------------------------------------------------------- A-32: the public edge is unchanged
KEYS = {"acting_principal": "intruder", "session": "sess-forged"}


def _public_event(name, args, block=None):
    return {"requestContext": {"http": {"method": "POST", "path": "/mcp"}}, "rawPath": "/mcp",
            "headers": {"content-type": "application/json", "accept": "application/json, text/event-stream"},
            "body": json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": args}}),
            "isBase64Encoded": False,
            "io_kit_mcp": block or {"principal": {"sub": "owner-sub", "groups": []}, "pool": "iograph"}}


@pytest.fixture
def public(internal, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "public")
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", [])
    monkeypatch.setenv("IOGRAPH_ALLOWED_SUBS", "owner-sub")
    yield internal


@pytest.mark.parametrize("where", ["top", "block", "forged-internal-pool", "all"])
def test_public_edge_ignores_the_two_keys_wherever_they_sit(public, where):
    ev = _public_event("create_lodging", STAY)
    if where in ("top", "all"):
        ev.update(KEYS)
    if where in ("block", "all"):
        ev["io_kit_mcp"] = {"principal": {"sub": "owner-sub", "groups": [], **KEYS}, "pool": "iograph", **KEYS}
    if where == "forged-internal-pool":
        ev["io_kit_mcp"] = {"principal": {"sub": "owner-sub", "groups": [], **KEYS}, "pool": "internal-door", **KEYS}
        ev.update(KEYS)
    out = body(lf._edge_inner(ev))
    assert out["success"] is True, out
    (item,) = rows(public)
    stored = json.dumps(item, default=str)
    assert item["author"] == "owner-sub" and "session" not in item
    assert "intruder" not in stored and "sess-forged" not in stored and lf._ACTING_SESSION.get() == ""


def test_public_edge_audit_line_keeps_its_keys(public, caplog):
    ev = _public_event("search_flights", {})
    ev.update(KEYS)
    with caplog.at_level(logging.INFO):
        lf._edge_inner(ev)
    (line,) = audit_lines(caplog)
    assert set(line) == {"audit", "tool", "principal", "principal_kind", "idempotency_key", "allowed", "ts"}


def test_public_write_still_follows_write_groups(public, monkeypatch):
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", ["writers"])
    ev = _public_event("create_lodging", STAY)
    ev.update(KEYS)
    assert body(lf._edge_inner(ev))["success"] is False and rows(public) == []


def test_contract_still_pins_the_seams():
    assert len(contract.ERROR_CODES) == 7 and len(contract.SETTINGS) == 13
