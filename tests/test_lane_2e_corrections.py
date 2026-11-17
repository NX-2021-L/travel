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
        boto3.resource("dynamodb", region_name="us-west-2").create_table(
            TableName=os.environ["TABLE_NAME"], BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}])
        principal = {"kind": "cognito", "id": "io@example.test", "sub": "s", "groups": [], "allowed": True}
        tok = lf._PRINCIPAL.set(principal)
        yield t
        lf._PRINCIPAL.reset(tok)


def seed(*ids):
    """A correction names only records that exist (FLY-TSK-045): put a minimal row for each id."""
    d = boto3.resource("dynamodb", region_name="us-west-2")
    for rid in ids:
        kind, _, rest = rid.partition("-")
        if kind in ("lodging", "car_rental", "drive") and rest:
            d.Table(RECORDS).put_item(Item={"pk": f"{kind}#{rest}", "sk": "rec", "kind": kind, "record_id": rid})
        else:
            d.Table(os.environ["TABLE_NAME"]).put_item(Item={"flight_id": rid})


def call(tool, args):
    if tool == "record_trip_correction" and not args.get("_unseeded"):
        seed(*[r for r in args.get("record_ids") or [] if isinstance(r, str)])
    args = {k: v for k, v in args.items() if k != "_unseeded"}
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
        seed("a")
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
        out = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a"], "_unseeded": True})
    finally:
        lf._PRINCIPAL.reset(tok)
    assert out["error_envelope"]["code"] == "missing" and "RECORDS_TABLE_NAME" in out["error"]


def test_unreachable_table_fails_by_name(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", "no-such-table")
    with mock_aws():
        tok = lf._PRINCIPAL.set({"kind": "cognito", "id": "io", "sub": "s", "groups": [], "allowed": True})
        try:
            out = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a"], "_unseeded": True})
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
    assert set(a[0]) == {"correction_id", "kind", "date", "date_iso", "record_ids", "reason", "author", "session",
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
    (c,) = t["corrections"]
    assert c["correction_id"] == cid and c["status"] == "active" and c["created_at"]
    assert {k: c[k] for k in ("kind", "date", "date_iso", "record_ids", "author", "question_id")} == {
        "kind": "returned", "date": "2024-03-06", "date_iso": "2024-03-06", "record_ids": ["f1"],
        "author": "io@example.test", "question_id": None}

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
    """(Seeded: the write refuses an absent id, see test_unknown_ids_are_refused.) A correction whose record is not among the engine's inputs is unapplied."""
    record({"kind": "returned", "date": "2024-03-06", "record_ids": ["no-such-flight"]})
    res = _trips(OPEN, table)
    assert [u["record_ids"] for u in res["unapplied_corrections"]] == [["no-such-flight"]]
    assert res["trips"][0]["last_basis"] == "undecided"                          # the trip is untouched


# ================================================================== FLY-TSK-045 (lane 3E): A-35, A-36, A-37
def _conf(ids, first, last, **kw):
    return {"kind": "confirmed", "record_ids": ids, "first_date": first, "last_date": last, "method": "examined", **kw}


def _ec(cid, kind, ids, status="active", created="2024-06-01T00:00:00.000000Z", **kw):
    return {"correction_id": cid, "kind": kind, "record_ids": ids, "status": status, "created_at": created, **kw}


OPEN3 = [_fl("o1", "2024-06-01", _SEA, _JFK)]  # out in June, no way back recorded


def _strip(res):
    """The trips without the two fields a confirmation may change."""
    return [{k: v for k, v in t.items() if k not in ("arc", "confirmation_id")} for t in res["trips"]]


@pytest.mark.parametrize("args", [
    {"kind": "returned", "date": "2024-03-06", "record_ids": ["ghost"]},
    {"kind": "left", "date": "2024-03-01", "record_ids": ["ghost"]},
    {"kind": "together", "record_ids": ["f1", "ghost"]},
    {"kind": "leave_out", "record_ids": ["ghost", "drive-ghost"]},
    _conf(["ghost"], "2024-03-01", "2024-03-02"),
])
def test_unknown_record_ids_are_refused_naming_them_dry_run_included(table, args):
    seed("f1")
    for dry in (True, False):
        out = call("record_trip_correction", {**args, "dry_run": dry, "_unseeded": True})
        assert out["success"] is False and out["error_envelope"]["code"] == "invalid" and "ghost" in out["error"]
    assert rows(table) == []


def test_a_cancelled_record_is_held_and_accepted(table):
    table.put_item(Item={"pk": "lodging#x", "sk": "rec", "kind": "lodging", "status": "cancelled"})
    assert record({"kind": "leave_out", "record_ids": ["lodging-x"], "_unseeded": True})["status"] == "active"


def test_answer_trip_question_checks_the_ids_before_writing():
    import inspect
    import lane_3b_repair
    src = inspect.getsource(lane_3b_repair.answer_trip_question)
    assert src.index("check_record_ids_exist") < src.index('record = corrections_lane.HANDLERS["record_trip_correction"]')


def test_engine_reports_unknown_ids_for_every_kind_leave_out_included():
    cs = [_ec("c-out", "leave_out", ["ghost"]), _ec("c-ok", "leave_out", ["a1"]),
          _ec("c-ret", "returned", ["a2", "ghost2"], date="2024-03-02")]
    res = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=cs)
    got = {u["correction_id"]: u["unknown_record_ids"] for u in res["unapplied_corrections"]}
    assert got == {"c-out": ["ghost"], "c-ret": ["ghost2"]}
    assert te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[cs[1]])["unapplied_corrections"] == []


def test_the_confirmed_row_is_written_in_two_steps_with_its_author_retired_and_never_deleted(table):
    args = _conf(["a1", "a2"], "2024-03-01", "2024-03-02", method="accepted_list")
    seed("a1", "a2")
    dry = call("record_trip_correction", {**args, "dry_run": True})
    assert dry["result"]["resolved_call"]["underlying"][0]["body"]["author"] == "io@example.test" and rows(table) == []
    res = record(args, session="s1")
    assert (res["kind"], res["first_date"], res["last_date"], res["method"], res["supersedes"], res["author"]) == \
        ("confirmed", "2024-03-01", "2024-03-02", "accepted_list", [], "io@example.test")
    (item,) = rows(table)
    assert item["correction_kind"] == "confirmed" and item["method"] == "accepted_list" and "supersedes" not in item
    ret = json.loads(asyncio.run(lf.call_tool("retire_trip_correction", {"correction_id": res["correction_id"]}))[0].text)
    assert ret["result"]["status"] == "retired" and len(rows(table)) == 1


@pytest.mark.parametrize("bad", [
    {"first_date": None}, {"last_date": "nope"}, {"first_date": "2024-03-03"}, {"method": "guess"}, {"method": None},
    {"supersedes": "correction-abc"}, {"supersedes": ["lodging-x"]},
])
def test_confirmed_validation(table, bad):
    out = call("record_trip_correction", {**_conf(["a1"], "2024-03-01", "2024-03-02"), **bad})
    assert out["error_envelope"]["code"] == "invalid" and rows(table) == []


def test_the_confirmed_fields_belong_to_that_kind_only(table):
    out = call("record_trip_correction", {"kind": "leave_out", "record_ids": ["a1"], "method": "examined"})
    assert out["error_envelope"]["code"] == "invalid" and rows(table) == []


def test_supersedes_names_only_existing_confirmations_retired_ones_allowed(table):
    first = record(_conf(["a1", "a2"], "2024-03-01", "2024-03-02"))
    other = record({"kind": "leave_out", "record_ids": ["a1"]})
    for bad in (["correction-0000000000000000"], [other["correction_id"]]):
        for dry in (True, False):
            out = call("record_trip_correction", {**_conf(["a1", "a2"], "2024-03-01", "2024-03-02"), "supersedes": bad, "dry_run": dry})
            assert out["error_envelope"]["code"] == "invalid" and bad[0] in out["error"]
    json.loads(asyncio.run(lf.call_tool("retire_trip_correction", {"correction_id": first["correction_id"]}))[0].text)
    n = len(rows(table))
    ok = record({**_conf(["a1", "a2"], "2024-03-01", "2024-03-02"), "supersedes": [first["correction_id"]]})
    assert ok["supersedes"] == [first["correction_id"]] and len(rows(table)) == n + 1


def test_a_confirmation_changes_no_trip_and_two_reads_agree():
    plain = te.compute_trips(TWO + OPEN3, [_HOME], today="2024-12-01")
    c = _ec("c1", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-02", method="examined")
    off = _ec("c2", "confirmed", ["b1", "b2"], first_date="2024-03-03", last_date="2024-03-06", method="examined")  # matches nothing
    with_c = te.compute_trips(TWO + OPEN3, [_HOME], today="2024-12-01", corrections=[c, off])
    again = te.compute_trips(TWO + OPEN3, [_HOME], today="2024-12-01", corrections=[off, c])
    assert _strip(plain) == _strip(with_c) and with_c == again
    assert plain["excluded"] == with_c["excluded"] and [t["key"] for t in plain["trips"]] == [t["key"] for t in with_c["trips"]]
    assert all(t["corrections"] == [] for t in with_c["trips"])


def test_arc_confirmed_proposed_detected_and_state_untouched():
    c = _ec("correction-c1", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-02", method="examined")
    res = te.compute_trips(TWO + OPEN3, [_HOME], today="2024-12-01", corrections=[c])
    by = {t["key"]: t for t in res["trips"]}
    assert (by["a1"]["arc"], by["a1"]["confirmation_id"]) == ("confirmed", "correction-c1")
    assert (by["b1"]["arc"], by["b1"]["confirmation_id"]) == ("proposed", None)
    assert (by["o1"]["arc"], by["o1"]["flags"]) == ("detected", ["return not recorded"])
    assert {t["state"] for t in res["trips"]} == {"past"} and res["unapplied_corrections"] == []


def test_arc_is_detected_with_a_signal_and_proposed_never_with_a_flag():
    dup = [_fl("d1", "2024-03-01", _SEA, _JFK), _fl("d2", "2024-03-01", _SEA, _JFK), _fl("d3", "2024-03-02", _JFK, _SEA)]
    res = te.compute_trips(dup, [_HOME], today="2024-12-01")
    assert all(t["signals"] and t["arc"] == "detected" for t in res["trips"] if t["signals"])
    assert all(t["arc"] == ("proposed" if not (t["flags"] or t["signals"]) else "detected") for t in res["trips"])


def test_a_confirmation_that_matches_no_trip_is_unapplied_and_not_dropped():
    wrong_dates = _ec("c-d", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-09", method="examined")
    wrong_set = _ec("c-s", "confirmed", ["a1"], first_date="2024-03-01", last_date="2024-03-02", method="examined")
    res = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[wrong_dates, wrong_set])
    assert sorted(u["correction_id"] for u in res["unapplied_corrections"]) == ["c-d", "c-s"]
    assert all(t["arc"] == "proposed" for t in res["trips"])


def test_adding_or_leaving_out_a_record_turns_the_confirmation_unapplied():
    c = _ec("c1", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-02", method="examined")
    assert te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[c])["unapplied_corrections"] == []
    lodging = TWO + [{"record_id": "lodging-z", "kind": "lodging", "first_night": "2024-03-01", "last_night": "2024-03-01",
                      "place_id": "city:new-york", "area_id": "city:new-york", "status": "active"}]
    res = te.compute_trips(lodging, [_HOME], today="2024-12-01", corrections=[c])
    assert [u["correction_id"] for u in res["unapplied_corrections"]] == ["c1"]
    out = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[c, _ec("c-lo", "leave_out", ["a2"])])
    assert "c1" in [u["correction_id"] for u in out["unapplied_corrections"]]


def test_a_superseded_confirmation_is_ignored_and_not_reported():
    old = _ec("correction-old", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-02", method="examined")
    new = _ec("correction-new", "confirmed", ["a1", "a2"], first_date="2024-03-01", last_date="2024-03-02", method="examined",
              supersedes=["correction-old"], created="2024-06-02T00:00:00.000000Z")
    res = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[old, new])
    assert res["trips"][0]["confirmation_id"] == "correction-new" and res["unapplied_corrections"] == []
    stale = _ec("correction-s", "confirmed", ["b1"], first_date="2024-03-03", last_date="2024-03-05", method="examined")
    newer = _ec("correction-n", "confirmed", ["b1", "b2"], first_date="2024-03-03", last_date="2024-03-05", method="examined",
                supersedes=["correction-s"])
    res = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=[stale, newer])
    assert res["unapplied_corrections"] == []          # the superseded one does not match, and is not reported either


def test_several_confirmations_on_one_trip_newest_wins_and_the_id_breaks_a_tie():
    kw = dict(first_date="2024-03-01", last_date="2024-03-02", method="examined")
    a = _ec("correction-a", "confirmed", ["a1", "a2"], created="2024-06-01T00:00:00.000000Z", **kw)
    b = _ec("correction-b", "confirmed", ["a1", "a2"], created="2024-06-01T00:00:00.000000Z", **kw)
    c = _ec("correction-c", "confirmed", ["a1", "a2"], created="2024-05-01T00:00:00.000000Z", **kw)
    for order in ([a, b, c], [c, b, a], [b, a, c]):
        res = te.compute_trips(TWO, [_HOME], today="2024-12-01", corrections=order)
        assert res["trips"][0]["confirmation_id"] == "correction-b" and res["unapplied_corrections"] == []
