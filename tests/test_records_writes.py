"""FLY-TSK-087 (BRD DOC-D14184D324F0 6.7, T-28, T-29): the write half of the records door, trip corrections only.

Offline: moto tables named here, the REAL trip engine over the synthetic records of tests/records_fixture.py, and a
synthetic door key. Every value is synthetic (tests/test_owner_marker_guard.py).
"""
import ast
import asyncio
import json
import sys
import types
from datetime import timedelta
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parent))
import records_fixture as fx  # noqa: E402,F401  (puts travel_mcp on sys.path)
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
import lane_2e_corrections as lane  # noqa: E402
import lane_3a_trips  # noqa: E402
import records_routes as rr  # noqa: E402
import records_writes as rw  # noqa: E402
import trip_engine  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TL = ROOT / "backend" / "lambda" / "travel_mcp"
RECORDS_TABLE = "fixture-records"
SUB, OTHER_SUB = "synthetic-sub", "fixture-other-sub"


@pytest.fixture(autouse=True)
def world(monkeypatch):
    for k, v in fx.env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS_TABLE)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setattr(lf, "_dynamo_resource", None)

    async def compute(today=None, near_home_km=None):  # the real engine over synthetic records and STORED corrections
        from datetime import date
        used = today or date.fromisoformat(fx.TODAY)
        return trip_engine.compute_trips(fx.RECORDS, [fx.HOME], today=used, corrections=lane.engine_corrections()), used

    monkeypatch.setattr(lane_3a_trips, "_compute", compute)
    monkeypatch.setattr(lane_3a_trips, "_today", lambda: __import__("datetime").date.fromisoformat(fx.TODAY))
    with mock_aws():
        d = boto3.resource("dynamodb", region_name=lf.REGION)
        d.create_table(TableName=RECORDS_TABLE, BillingMode="PAY_PER_REQUEST",
                       KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                       AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
        flights = d.create_table(TableName=lf.TABLE_NAME, BillingMode="PAY_PER_REQUEST",
                                 KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
                                 AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}])
        for r in fx.RECORDS:
            flights.put_item(Item={"flight_id": r["flight_id"]})
        yield d


def call(method, path, body=None, sub=SUB, key="fixture-key-1", token=None):
    h = {"Authorization": f"Bearer {token or fx.token(sub=sub)}"}
    if key:
        h["Idempotency-Key"] = key
    ev = {"requestContext": {"http": {"method": method, "path": path}}, "rawPath": path, "rawQueryString": "",
          "headers": h, "body": json.dumps(body) if body is not None else None}
    r = rr.handle(ev)
    return r["statusCode"], json.loads(r["body"])


INTENTS = "/records/recordsets/trip_corrections/intents"
WRITES = "/records/recordsets/trip_corrections/writes"
RETURNED = {"kind": "returned", "date": "2024-10-09", "record_ids": ["F-BACK1"]}


def create(patch=None, key="fixture-key-1", reason="fixture reason", sub=SUB):
    body = {"kind": "create", "patch": patch or RETURNED, **({"reason": reason} if reason is not None else {})}
    return call("POST", INTENTS, body, sub=sub, key=key)


def trips():
    return call("GET", "/records/recordsets/trips/rows", key=None)[1]["rows"]


def writes(sub=SUB):
    return call("GET", WRITES, sub=sub, key=None)[1]


# ---------------------------------------------------------------- criterion 1: the descriptor and the mapping
def test_trip_corrections_declares_create_and_retire_with_reason_required():
    d = call("GET", "/records/recordsets/trip_corrections/descriptor", key=None)[1]
    assert d["capabilities"] == ["create", "retire"] and d["reason"] == "required"
    editable = {c["id"] for c in d["columns"] if c["editable"]}
    assert editable == set(rw.CREATE_FIELDS)
    t = call("GET", "/records/recordsets/trips/descriptor", key=None)[1]
    assert t["capabilities"] == [] and t["class"] == "derived"


def test_create_goes_to_the_handler_with_the_door_subject_as_author_and_the_key(world):
    status, rc = create()
    assert status == 200 and rc["status"] == "applied"
    row = rc["rows"][0]
    assert row["values"]["author"] == SUB and row["values"]["kind"] == "returned" and row["values"]["reason"] == "fixture reason"
    held = world.Table(RECORDS_TABLE).scan()["Items"]
    assert len(held) == 1 and held[0]["idempotency_key"] == "fixture-key-1" and held[0]["door"] == "human"
    assert rc["refreshed"] == ["trip_corrections", "trips"] and rc["undo"]["token"]


def test_a_replayed_create_returns_the_same_row_and_writes_once(world):
    first, again = create()[1], create()[1]
    assert again["status"] == "applied" and again["rows"][0]["key"] == first["rows"][0]["key"]
    assert len(world.Table(RECORDS_TABLE).scan()["Items"]) == 1
    status, reused = create(patch={**RETURNED, "date": "2024-10-10"})  # the same key, another intent
    assert status == 422 and reused["status"] == "rejected"


def test_retire_needs_the_rows_version_and_goes_to_the_retire_handler():
    row = create()[1]["rows"][0]
    status, stale = call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": "stale", "reason": "fixture"}, key="fixture-key-2")
    assert status == 409 and stale["status"] == "conflict" and stale["rows"][0]["values"]["status"] == "active"
    status, rc = call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture"}, key="fixture-key-2")
    assert status == 200 and rc["rows"][0]["values"]["status"] == "retired" and rc["rows"][0]["values"]["retired_by"] == SUB
    assert rc["undo"] is None  # no handler restores a retired correction


# ---------------------------------------------------------------- criterion 2 (T-28): the handlers are the only write path
WRITE_CALLS = {"put_item", "update_item", "delete_item", "batch_writer", "batch_write_item", "transact_write_items",
               "execute_statement", "Table", "resource", "client"}


@pytest.mark.parametrize("module", ["records_routes.py", "records_writes.py", "records_door.py"])
def test_no_record_route_writes_the_table_itself(module):
    tree = ast.parse((TL / module).read_text())
    imports = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
               for a in (n.names if isinstance(n, ast.Import) else [ast.alias(n.module or "")])}
    assert not imports & {"boto3", "botocore"}
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not calls & WRITE_CALLS
    lane_uses = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                 and n.value.id in ("corrections", "lane_2e_corrections")}
    assert lane_uses <= {"record_trip_correction", "retire_trip_correction", "load_corrections", "load_correction_audit",
                         "door_write", "_lf"}


def test_every_records_door_write_runs_inside_one_of_the_two_handlers(world, monkeypatch):
    import inspect

    from botocore.client import BaseClient
    seen, real = [], BaseClient._make_api_call
    writes_ops = {"PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "TransactWriteItems", "ExecuteStatement"}

    def spy(self, op, params):
        if op in writes_ops:
            seen.append((op, {f.function for f in inspect.stack()} & {"record_trip_correction", "retire_trip_correction"}))
        return real(self, op, params)
    monkeypatch.setattr(BaseClient, "_make_api_call", spy)
    rc = create()[1]
    row = rc["rows"][0]
    call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture"}, key="fixture-key-2")
    second = create(patch={"kind": "leave_out", "record_ids": ["F-OUT1"]}, key="fixture-key-6")[1]
    call("POST", INTENTS, {"kind": "undo", "undo_token": second["undo"]["token"]}, key="fixture-key-7")
    assert [op for op, _ in seen] == ["PutItem", "UpdateItem", "PutItem", "UpdateItem"] and all(h for _, h in seen)


# ---------------------------------------------------------------- criterion 3: the writes route and undo
def test_writes_serve_create_and_retire_with_reason_principal_and_door():
    row = create()[1]["rows"][0]
    call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture retire"}, key="fixture-key-2")
    log = writes()
    retire, made = log
    assert made == {**made, "kind": "create", "key": row["key"], "idempotency_key": "fixture-key-1", "principal": SUB,
                    "door": "human", "reason": "fixture reason", "undo": None, "undo_closed": "changed"}
    assert retire == {**retire, "kind": "retire", "key": row["key"], "idempotency_key": "fixture-key-2", "principal": SUB,
                      "door": "human", "reason": "fixture retire", "undo": None, "undo_closed": "no_inverse"}
    assert {e["door"] for e in log} <= {"human", "agent"} and not {e.get("undo_closed") for e in log} & {"service", "settled"}


def test_undo_of_a_create_is_a_retire_and_is_logged_as_an_undo():
    rc = create()[1]
    entry = writes()[0]
    assert entry["undo"]["token"] == rc["undo"]["token"]
    status, u = call("POST", INTENTS, {"kind": "undo", "undo_token": rc["undo"]["token"]}, key="fixture-key-3")
    assert status == 200 and u["status"] == "applied" and u["undo"] is None
    assert u["rows"][0]["values"]["status"] == "retired"
    undo, made = writes()
    assert undo["kind"] == "undo" and undo["undo_closed"] == "is_undo" and undo["undo"] is None
    assert made["undo"] is None and made["undo_closed"] == "changed"
    assert call("POST", INTENTS, {"kind": "undo", "undo_token": rc["undo"]["token"]}, key="fixture-key-3")[1]["status"] == "applied"  # replay


def test_an_undo_token_is_the_creators_only_and_closes_with_the_window(monkeypatch):
    rc = create()[1]
    assert writes(sub=OTHER_SUB)[0]["undo"] is None
    status, other = call("POST", INTENTS, {"kind": "undo", "undo_token": rc["undo"]["token"]}, sub=OTHER_SUB, key="fixture-key-4")
    assert status == 422 and other["problems"][0]["field"] == "undo_token"
    later = rw._now() + rw.UNDO_WINDOW + timedelta(seconds=1)
    monkeypatch.setattr(rw, "_now", lambda: later)
    assert writes()[0]["undo"] is None and writes()[0]["undo_closed"] == "expired"
    status, late = call("POST", INTENTS, {"kind": "undo", "undo_token": rc["undo"]["token"]}, key="fixture-key-5")
    assert status == 422 and late["status"] == "rejected"
    assert writes()[0]["kind"] == "create"  # nothing was written


def test_a_retire_offers_no_undo():
    row = create()[1]["rows"][0]
    rc = call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture"}, key="fixture-key-2")[1]
    assert rc["undo"] is None and writes()[0]["undo_closed"] == "no_inverse"


def test_an_mcp_write_is_served_as_the_agent_door(world):
    tok = lf._PRINCIPAL.set({"kind": "cognito", "id": "fixture-agent-principal", "sub": "s", "groups": [], "allowed": True})
    try:
        out = json.loads(asyncio.run(lane.record_trip_correction({**RETURNED, "reason": "fixture"}))[0].text)
    finally:
        lf._PRINCIPAL.reset(tok)
    assert out["success"]
    entry = writes()[0]
    assert entry["door"] == "agent" and entry["principal"] == "fixture-agent-principal" and entry["undo"] is None


# ---------------------------------------------------------------- criterion 4 (T-29): field problems, idempotent retire
@pytest.mark.parametrize("patch,reason,field", [
    (RETURNED, None, "reason"),
    (RETURNED, "  ", "reason"),
    ({"kind": "returned", "record_ids": ["F-BACK1"]}, "fixture", "date"),
    ({"kind": "left", "record_ids": ["F-OUT1"]}, "fixture", "date"),
    ({"kind": "together", "record_ids": ["F-OUT1"]}, "fixture", "record_ids"),
    ({"kind": "confirmed", "record_ids": ["F-OUT1"], "last_date": "2024-10-08", "method": "examined"}, "fixture", "first_date"),
    ({"kind": "confirmed", "record_ids": ["F-OUT1"], "first_date": "2024-10-01", "method": "examined"}, "fixture", "last_date"),
    ({"kind": "confirmed", "record_ids": ["F-OUT1"], "first_date": "2024-10-01", "last_date": "2024-10-08"}, "fixture", "method"),
    ({"kind": "sideways", "record_ids": ["F-OUT1"]}, "fixture", "kind"),
    ({"kind": "leave_out", "record_ids": ["F-NOWHERE"]}, "fixture", "record_ids"),
    ({**RETURNED, "status": "retired"}, "fixture", "status"),
])
def test_create_refuses_a_missing_field_as_a_field_problem_and_writes_nothing(world, patch, reason, field):
    status, rc = create(patch=patch, reason=reason)
    assert status == 422 and rc["status"] == "rejected" and rc["problems"][0]["field"] == field
    assert rc["problems"][0]["status"] == 422 and rc["rows"] == []
    assert world.Table(RECORDS_TABLE).scan()["Items"] == []


def test_retire_is_idempotent(world):
    row = create()[1]["rows"][0]
    body = {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture"}
    first = call("POST", INTENTS, body, key="fixture-key-2")[1]
    again = call("POST", INTENTS, body, key="fixture-key-2")[1]
    assert first["status"] == again["status"] == "applied" and again["rows"] == first["rows"]
    assert [e["kind"] for e in writes()] == ["retire", "create"]


def test_an_intent_needs_a_key_and_a_kind_this_set_takes():
    assert call("POST", INTENTS, {"kind": "create", "patch": RETURNED, "reason": "x"}, key=None)[0] == 400
    status, rc = call("POST", INTENTS, {"kind": "amend", "key": "x", "if_version": 1, "patch": {}, "reason": "x"})
    assert status == 422 and rc["problems"][0]["field"] == "kind"
    assert call("POST", "/records/recordsets/trips/intents", {"kind": "recompute"})[0] == 405
    assert call("POST", INTENTS, {"kind": "create"}, token="not-a-token")[0] == 401


# ---------------------------------------------------------------- criterion 5: trips stay computed on read
def test_a_correction_shows_in_the_next_trips_read_and_a_retired_one_leaves_it_equal():
    before = trips()
    rc = create(patch={"kind": "leave_out", "record_ids": ["F-OUT2", "F-BACK2"]})[1]
    after = trips()
    assert after != before
    row = rc["rows"][0]
    call("POST", INTENTS, {"kind": "retire", "key": row["key"], "if_version": row["version"], "reason": "fixture"}, key="fixture-key-2")
    assert trips() == before
