"""FLY-TSK-029-0E / A-11: corrections are written in two steps with their author, survive a rebuild,
reopen their trip when retired, and are never deleted.

Offline (moto). Clauses 3 and 4 are proved against the REAL engine (trip_engine.compute_trips) in the
test_real_engine_* tests at the bottom (FLY-TSK-042). The `standin_engine` tests above predate lane 2D and
are kept as a cheap engine-independent check of the storage surface.
"""
import ast
import asyncio
import json
import os
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("TABLE_NAME", "io-travel-flights")
MCP_DIR = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(MCP_DIR))
import contract  # noqa: E402
import lambda_function as lf  # noqa: E402
import lane_2e_corrections as lane  # noqa: E402

RECORDS = "io-travel-records-test"


@pytest.fixture
def table(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS)
    with mock_aws():
        t = boto3.resource("dynamodb", region_name="us-west-2").create_table(
            TableName=RECORDS,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        principal = {"kind": "cognito", "id": "io@example.test", "sub": "s", "groups": [], "allowed": True}
        tok = lf._PRINCIPAL.set(principal)
        yield t
        lf._PRINCIPAL.reset(tok)


def call(tool, args):
    return json.loads(asyncio.run(lf.call_tool(tool, args))[0].text)


def record(args, **extra):
    out = call("record_trip_correction", {**args, **extra})
    assert out["success"] is True, out
    return out["result"]


def rows(t):
    return t.scan()["Items"]


def test_tools_are_wired_not_stubbed():
    assert lf._TOOL_HANDLERS["record_trip_correction"] is lane.record_trip_correction
    assert lf._TOOL_HANDLERS["retire_trip_correction"] is lane.retire_trip_correction


# ------------------------------------------------------------------ the four kinds
@pytest.mark.parametrize("args", [
    {"kind": "returned", "date": "2026-03-10", "record_ids": ["flight-1"]},
    {"kind": "left", "date": "2026-03-01", "record_ids": ["flight-1"]},
    {"kind": "together", "record_ids": ["lodging-b", "flight-a"]},
    {"kind": "leave_out", "record_ids": ["drive-9"], "reason": "not my travel"},
])
def test_four_kinds_write_a_correction_row(table, args):
    res = record(args)
    assert res["kind"] == args["kind"] and res["status"] == "active"
    assert res["correction_id"].startswith("correction-")
    item = table.get_item(Key={"pk": "correction#" + res["correction_id"][len("correction-"):], "sk": "rec"})["Item"]
    assert item["kind"] == "correction" and item["correction_kind"] == args["kind"] and item["date_iso"]
    assert item["record_ids"] == sorted(args["record_ids"])
    assert item["pk"] == contract.record_keys("correction", [args["kind"], args.get("date", ""),
                                                             ",".join(sorted(args["record_ids"])), item["created_at"]])["pk"]


@pytest.mark.parametrize("args", [
    {"kind": "bogus", "record_ids": ["a"]},
    {"kind": "returned", "record_ids": ["a"]},                       # date required
    {"kind": "left", "date": "not-a-date", "record_ids": ["a"]},
    {"kind": "together", "record_ids": ["only-one"]},
    {"kind": "leave_out", "record_ids": []},
])
def test_invalid_corrections_rejected_without_writing(table, args):
    out = call("record_trip_correction", args)
    assert out["error_envelope"]["code"] == "invalid" and rows(table) == []


# ------------------------------------------------------------------ clause 1: two steps
def test_dry_run_reports_and_writes_nothing_then_real_call_writes(table):
    args = {"kind": "returned", "date": "2026-03-10", "record_ids": ["flight-1"], "session": "sess-1"}
    out = call("record_trip_correction", {**args, "dry_run": True})
    r = out["result"]["resolved_call"]
    assert r["source"] == "server-dry-run" and r["sideEffects"] == {"writeCount": 1, "kinds": ["dynamodb:PutItem"]}
    assert r["underlying"][0]["body"]["author"] == "io@example.test"
    assert rows(table) == []                                   # step one changed nothing
    assert record(args)["status"] == "active" and len(rows(table)) == 1   # step two wrote


# ------------------------------------------------------------------ clause 2: author
def test_author_stamped_with_and_without_session(table):
    with_s = record({"kind": "leave_out", "record_ids": ["a"]}, session="sess-7")
    without = record({"kind": "leave_out", "record_ids": ["b"]})
    assert with_s["author"] == "io@example.test" and with_s["session"] == "sess-7"
    assert without["author"] == "io@example.test" and without["session"] is None
    stored = {i["record_ids"][0]: i for i in rows(table)}
    assert "session" not in stored["b"] and stored["a"]["session"] == "sess-7"


def test_author_is_server_stamped_not_accepted_as_input(table):
    res = record({"kind": "leave_out", "record_ids": ["a"], "author": "mallory"})
    assert res["author"] == "io@example.test"


def test_no_principal_no_correction(table):
    tok = lf._PRINCIPAL.set(None)
    try:
        gated = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a"]})
        direct = json.loads(asyncio.run(lane.record_trip_correction({"kind": "leave_out", "record_ids": ["a"]}))[0].text)
    finally:
        lf._PRINCIPAL.reset(tok)
    assert gated["error_envelope"]["code"] == "forbidden"          # the dispatcher refuses first
    assert direct["error_envelope"]["code"] == "not_allowed" and rows(table) == []   # and so does the handler


# ------------------------------------------------------------------ runtime caveat
def test_unset_setting_fails_by_name(monkeypatch):
    monkeypatch.delenv("RECORDS_TABLE_NAME", raising=False)
    tok = lf._PRINCIPAL.set({"kind": "cognito", "id": "io", "sub": "s", "groups": [], "allowed": True})
    try:
        out = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a"]})
    finally:
        lf._PRINCIPAL.reset(tok)
    assert out["error_envelope"]["code"] == "missing" and "RECORDS_TABLE_NAME" in out["error"]


def test_unreachable_table_fails_by_name(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", "no-such-table")
    with mock_aws():
        tok = lf._PRINCIPAL.set({"kind": "cognito", "id": "io", "sub": "s", "groups": [], "allowed": True})
        try:
            out = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a"]})
        finally:
            lf._PRINCIPAL.reset(tok)
    assert out["error_envelope"]["code"] == "missing" and "RECORDS_TABLE_NAME" in out["error"]


# ------------------------------------------------------------------ stand-in engine (NOT lane 2D's)
RECS = [{"id": "f1", "date": "2026-03-01"}, {"id": "f2", "date": "2026-03-04"},
        {"id": "g1", "date": "2026-05-01"}]


def standin_engine(records, corrections):
    """Groups by a 20-day gap; corrections are inputs: leave_out drops, together merges, returned/left set a
    recorded boundary. Open trips have last_basis 'undecided'. Never reads anything but its arguments."""
    drop = {i for c in corrections if c["kind"] == "leave_out" for i in c["record_ids"]}
    recs = sorted((r for r in records if r["id"] not in drop), key=lambda r: r["date"])
    groups = []
    for r in recs:
        if groups and (_d(r["date"]) - _d(groups[-1][-1]["date"])).days <= 20:
            groups[-1].append(r)
        else:
            groups.append([r])
    for c in corrections:
        if c["kind"] == "together":
            hit = [g for g in groups if any(r["id"] in c["record_ids"] for r in g)]
            if len(hit) > 1:
                merged = sorted(sum(hit, []), key=lambda r: r["date"])
                groups = [g for g in groups if g not in hit] + [merged]
    trips = []
    for g in sorted(groups, key=lambda g: g[0]["date"]):
        t = {"key": g[0]["id"], "ids": [r["id"] for r in g], "last_date": g[-1]["date"], "last_basis": "undecided"}
        for c in corrections:
            if c["kind"] == "returned" and set(c["record_ids"]) & set(t["ids"]):
                t["last_date"], t["last_basis"] = c["date"], "recorded"
        trips.append(t)
    return trips


def _d(s):
    from datetime import date
    return date.fromisoformat(s)


# ------------------------------------------------------------------ clause 3: survives a rebuild
def test_correction_survives_a_rebuild(table):
    before = standin_engine(RECS, lane.engine_corrections(table))
    assert before[0]["last_basis"] == "undecided"
    record({"kind": "returned", "date": "2026-03-10", "record_ids": ["f2"]})
    record({"kind": "leave_out", "record_ids": ["g1"]})
    first = standin_engine(RECS, lane.engine_corrections(table))
    # a "rebuild": nothing carried over in memory, a fresh read of storage, same records
    second = standin_engine(RECS, lane.engine_corrections(boto3.resource("dynamodb", region_name="us-west-2").Table(RECORDS)))
    assert first == second
    assert first == [{"key": "f1", "ids": ["f1", "f2"], "last_date": "2026-03-10", "last_basis": "recorded"}]


def test_engine_surface_is_deterministic_and_carries_the_contract_fields(table):
    record({"kind": "together", "record_ids": ["f1", "g1"], "question_id": "q_x"})
    record({"kind": "left", "date": "2026-03-01", "record_ids": ["f1"]})
    a, b = lane.engine_corrections(table), lane.engine_corrections(table)
    assert a == b and [c["created_at"] for c in a] == sorted(c["created_at"] for c in a)
    assert set(a[0]) == {"correction_id", "kind", "date", "record_ids", "reason", "author", "session",
                         "question_id", "status", "created_at", "retired_at", "retired_by"}


# ------------------------------------------------------------------ clause 4: retire reopens; nothing deleted
def test_retire_reopens_the_trip_and_keeps_the_row(table):
    pre = standin_engine(RECS, lane.engine_corrections(table))
    cid = record({"kind": "returned", "date": "2026-03-10", "record_ids": ["f2"]})["correction_id"]
    closed = standin_engine(RECS, lane.engine_corrections(table))
    assert closed != pre and closed[0]["last_basis"] == "recorded"

    dry = call("retire_trip_correction", {"correction_id": cid, "dry_run": True})
    assert dry["result"]["resolved_call"]["diff"][0]["after"] == "retired"
    assert lane.engine_corrections(table)[0]["status"] == "active"           # dry run changed nothing

    res = call("retire_trip_correction", {"correction_id": cid, "reason": "wrong day", "session": "s9"})["result"]
    assert res["status"] == "retired" and res["retired_by"] == "io@example.test" and res["retired_at"]
    assert standin_engine(RECS, lane.engine_corrections(table)) == pre       # reopened: pre-correction shape
    assert len(rows(table)) == 1                                              # not deleted
    assert lane.load_corrections(table, include_retired=True)[0]["status"] == "retired"
    assert rows(table)[0]["retire_reason"] == "wrong day" and rows(table)[0]["author"] == "io@example.test"


def test_retire_is_idempotent_and_reports_missing(table):
    cid = record({"kind": "leave_out", "record_ids": ["a"]})["correction_id"]
    first = call("retire_trip_correction", {"correction_id": cid})["result"]
    again = call("retire_trip_correction", {"correction_id": cid})["result"]
    assert again["retired_at"] == first["retired_at"] and len(rows(table)) == 1
    assert call("retire_trip_correction", {"correction_id": "correction-0000000000000000"})["error_envelope"]["code"] == "missing"
    assert call("retire_trip_correction", {"correction_id": "lodging-abc"})["error_envelope"]["code"] == "invalid"


def test_no_code_path_deletes_a_record():
    tree = ast.parse((MCP_DIR / "lane_2e_corrections.py").read_text())
    forbidden = {"delete_item", "batch_write_item", "batch_writer", "delete", "delete_table", "transact_write_items",
                 "DeleteItem", "DeleteRequest", "REMOVE"}
    seen = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            seen.add(node.attr)
        elif isinstance(node, ast.Name):
            seen.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            seen.update(w.strip(",") for w in node.value.split())
    assert not (forbidden & seen), forbidden & seen
    assert "put_item" in seen and "update_item" in seen        # the scan sees real calls, so it is not vacuous


def test_no_delete_at_runtime(table, monkeypatch):
    """Belt and braces: make every delete path explode, then run the whole lifecycle."""
    from botocore.client import BaseClient
    orig = BaseClient._make_api_call

    def guard(self, op, params):
        assert not op.startswith("Delete") and op not in ("BatchWriteItem", "TransactWriteItems"), op
        return orig(self, op, params)
    monkeypatch.setattr(BaseClient, "_make_api_call", guard)
    cid = record({"kind": "leave_out", "record_ids": ["a"]})["correction_id"]
    call("retire_trip_correction", {"correction_id": cid})
    lane.load_corrections(table, include_retired=True)
    assert len(rows(table)) == 1


# ------------------------------------------------------------------ the real engine (lane 2D, FLY-TSK-042)
import trip_engine as te  # noqa: E402

_SEA, _JFK, _BOS = ("apt:SEA", "city:seattle"), ("apt:JFK", "city:new-york"), ("apt:BOS", "city:boston")
_HOME = {"residence_uid": "R1", "first_night": "2020-01-01", "last_night": None, "city": "Seattle",
         "places": [{"place_id": "apt:SEA", "area_id": "city:seattle"}]}


def _fl(fid, date, o, d):
    return {"flight_id": fid, "date_iso": date, "origin": o[0][4:], "dest": d[0][4:], "origin_place_id": o[0],
            "origin_area_id": o[1], "dest_place_id": d[0], "dest_area_id": d[1], "status": "active"}


def _trips(records, table):
    """The read path: stored corrections in, real engine out. Nothing carried over between calls."""
    return te.compute_trips(records, [_HOME], today="2024-12-01", corrections=lane.engine_corrections(table))


def _shape(res):
    return {k: res[k] for k in ("trips", "excluded", "unapplied_corrections", "duplicate_inputs")}


OPEN = [_fl("f1", "2024-03-01", _SEA, _JFK)]                                    # out, no way back recorded
TWO = [_fl("a1", "2024-03-01", _SEA, _JFK), _fl("a2", "2024-03-02", _JFK, _SEA),  # two separate closed trips
       _fl("b1", "2024-03-03", _SEA, _BOS), _fl("b2", "2024-03-05", _BOS, _SEA)]


def test_real_engine_returned_changes_trip_survives_rebuild_and_reopens_on_retire(table):
    pre = _shape(_trips(OPEN, table))
    assert pre["trips"][0]["last_basis"] == "undecided" and "return not recorded" in pre["trips"][0]["flags"]

    cid = record({"kind": "returned", "date": "2024-03-06", "record_ids": ["f1"], "session": "s1"})["correction_id"]
    t = _trips(OPEN, table)["trips"][0]
    assert (t["last_date"], t["last_basis"]) == ("2024-03-06", "recorded") and "return not recorded" not in t["flags"]
    assert t["corrections"] == [{"kind": "returned", "date": "2024-03-06", "record_ids": ["f1"],
                                 "author": "io@example.test", "question_id": None}]

    closed = _shape(_trips(OPEN, table))
    rebuilt = _shape(te.compute_trips(list(reversed(OPEN)), [_HOME], today="2024-12-01", corrections=lane.engine_corrections(
        boto3.resource("dynamodb", region_name="us-west-2").Table(RECORDS))))
    assert closed == rebuilt != pre                                              # same inputs, identical trips

    call("retire_trip_correction", {"correction_id": cid})
    assert _shape(_trips(OPEN, table)) == pre                                    # reopened: the pre-correction trips
    assert len(rows(table)) == 1                                                 # and the correction is still stored


def test_real_engine_leave_out_and_retire(table):
    pre = _shape(_trips(TWO, table))
    assert len(pre["trips"]) == 2
    cid = record({"kind": "leave_out", "record_ids": ["b1"]})["correction_id"]
    res = _trips(TWO, table)
    assert [x["record_id"] for x in res["excluded"] if x["disposition"] == "left_out"] == ["b1"]
    call("retire_trip_correction", {"correction_id": cid})
    assert _shape(_trips(TWO, table)) == pre


def test_real_engine_together_and_retire(table):
    pre = _shape(_trips(TWO, table))
    cid = record({"kind": "together", "record_ids": ["a1", "b1"]})["correction_id"]
    joined = _trips(TWO, table)
    assert len(joined["trips"]) == 1 and joined["trips"][0]["corrections"][0]["kind"] == "together"
    assert _shape(joined) == _shape(_trips(TWO, table))
    call("retire_trip_correction", {"correction_id": cid})
    assert _shape(_trips(TWO, table)) == pre


def test_real_engine_left_and_retire(table):
    only_back = [_fl("g1", "2024-03-05", _JFK, _SEA)]
    pre = _shape(_trips(only_back, table))
    cid = record({"kind": "left", "date": "2024-03-01", "record_ids": ["g1"]})["correction_id"]
    t = _trips(only_back, table)["trips"][0]
    assert (t["first_date"], t["first_basis"]) == ("2024-03-01", "recorded") and "start not recorded" not in t["flags"]
    call("retire_trip_correction", {"correction_id": cid})
    assert _shape(_trips(only_back, table)) == pre


def test_real_engine_reports_a_correction_naming_an_absent_record(table):
    """record_ids are not validated at write time (finding for lane 3B); the engine surfaces them as unapplied."""
    record({"kind": "returned", "date": "2024-03-06", "record_ids": ["no-such-flight"]})
    res = _trips(OPEN, table)
    assert [u["record_ids"] for u in res["unapplied_corrections"]] == [["no-such-flight"]]
    assert res["trips"][0]["last_basis"] == "undecided"                          # the trip is untouched
