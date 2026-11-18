"""A-13 (FLY-TSK-030-0B): prepared questions and guided repair, offline, against the REAL trip engine.

moto stands in for DynamoDB (flights and records tables), a stubbed transport stands in for finance. The
engine, lane 2E (the write path) and home_service are the real modules.
"""
import asyncio
import json
import os
import sys
from decimal import Decimal
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
import lane_2e_corrections as lane2e  # noqa: E402
import lane_3b_repair as lane  # noqa: E402
import trip_engine as te  # noqa: E402

RECORDS = "io-travel-records-test"
TODAY = "2024-12-01"
_SEA, _JFK, _BOS = ("apt:SEA", "city:seattle"), ("apt:JFK", "city:new-york"), ("apt:BOS", "city:boston")
PERIOD = {"residence_uid": "R1", "effective_from": "2020-01-01", "effective_to": None, "end_status": "ongoing",
          "tenure": "own", "state": "WA", "country": "US"}
ENGINE_HOME = [{"residence_uid": "R1", "first_night": "2020-01-01", "last_night": None}]


def finance(periods):
    body = json.dumps({"ok": True, "result": {"periods": periods}}).encode()
    return lambda function, payload, timeout: (None, body)


def fl(fid, date, o, d, **kw):
    return {"flight_id": fid, "date_iso": date, "origin": o[0][4:], "dest": d[0][4:], "origin_place_id": o[0],
            "origin_area_id": o[1], "dest_place_id": d[0], "dest_area_id": d[1], "status": "active", **kw}


def home_places(**place):
    return {**contract.record_keys("home_places", ["R1"]), "kind": "home_places", "status": "active", "residence_uid": "R1",
            "places": [{"place_id": "apt:SEA", "area_id": "city:seattle", **place}]}


@pytest.fixture
def world(monkeypatch):
    monkeypatch.setenv("RECORDS_TABLE_NAME", RECORDS)
    monkeypatch.setattr(lane, "_today", lambda: TODAY)
    monkeypatch.setattr(lane, "_HOME_KWARGS", {"invoker": finance([PERIOD]), "function": "finance-fn-test"})
    monkeypatch.setattr(lf, "_dynamo_resource", None)
    with mock_aws():
        db = boto3.resource("dynamodb", region_name="us-west-2")
        flights = db.create_table(TableName=lf.TABLE_NAME, KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
                                  AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}],
                                  BillingMode="PAY_PER_REQUEST")
        records = db.create_table(
            TableName=RECORDS, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST")
        records.put_item(Item=home_places())  # which places count as leaving home: the engine cannot tell without them
        tok = lf._PRINCIPAL.set({"kind": "cognito", "id": "io@example.test", "sub": "s", "groups": [], "allowed": True})

        class W:
            pass
        w = W()
        w.flights, w.records = flights, records

        def put(*items):
            for i in items:
                (records if "pk" in i else flights).put_item(Item=i)
        w.put = put
        yield w
        lf._PRINCIPAL.reset(tok)
        lf._dynamo_resource = None


def call(tool, args):
    return json.loads(asyncio.run(lf.call_tool(tool, args))[0].text)


def undecided(**args):
    out = call("list_undecided_trips", args)
    assert out["success"] is True, out
    return out["result"]


def rows(w):
    return [i for i in w.records.scan()["Items"] if i.get("kind") == "correction"]


OPEN = [fl("f1", "2024-03-01", _SEA, _JFK)]
ONLY_BACK = [fl("g1", "2024-03-05", _JFK, _SEA)]
LATER_PAIR = [fl("z1", "2024-07-01", _SEA, _JFK), fl("z2", "2024-07-02", _JFK, _SEA)]  # home is recorded from June
FUTURE_OPEN = [fl("h1", "2024-12-10", _SEA, _JFK)]


def _scenarios():
    near = {"pk": "lodging#near", "sk": "rec", "record_id": "lodging-near", "kind": "lodging", "status": "active", "name": "Inn",
            "first_night": "2024-03-01", "last_night": "2024-03-02", "date_iso": "2024-03-01", "place_id": "addr:inn",
            "area_id": "city:bellevue", "lat": Decimal("47.61"), "lon": Decimal("-122.20")}
    hp = home_places(lat=Decimal("47.45"), lon=Decimal("-122.30"))  # no radius: near, not listed
    short = [x for i in range(10) for x in (fl(f"s{i}a", f"2023-0{i % 9 + 1}-0{1 + i // 9}", _SEA, _BOS),
                                           fl(f"s{i}b", f"2023-0{i % 9 + 1}-0{2 + i // 9}", _BOS, _SEA))]
    return {
        "start not recorded": ONLY_BACK,
        "return not recorded": OPEN,
        "return not planned yet": FUTURE_OPEN,
        "likely duplicate": [fl("d1", "2024-03-01", _SEA, _JFK), fl("d2", "2024-03-02", _JFK, _BOS), fl("d3", "2024-03-02", _JFK, _BOS),
                             fl("d4", "2024-03-04", _BOS, _SEA)],
        "unusually long": short + [fl("L1", "2024-05-01", _SEA, _JFK), fl("L2", "2024-08-01", _JFK, _SEA)],
        "is this place home": [fl("n1", "2024-03-01", _SEA, _JFK), fl("n2", "2024-03-03", _JFK, _SEA), near, hp],
    }


# ------------------------------------------------------------------ clause 1: one question per flag and signal
def test_every_flag_and_signal_has_exactly_one_registered_question():
    names = list(contract.FLAGS) + list(contract.SIGNALS)
    assert set(contract.QUESTIONS) == set(names), "a flag or signal has no question, or a question has no flag"
    ids = [contract.QUESTIONS[n] for n in names]
    assert len(set(ids)) == len(ids), "two names share one question"
    assert set(ids) == set(lane._BUILDERS), "every registered question has wording and answers here, and nothing else does"


@pytest.mark.parametrize("name", list(contract.FLAGS) + list(contract.SIGNALS))
def test_each_question_has_two_to_four_answers_and_one_correction_kind_each(world, name):
    qid = contract.QUESTIONS[name]
    if name == "home not recorded for these dates":
        lane._HOME_KWARGS["invoker"] = finance([{**PERIOD, "effective_from": "2024-06-01"}])
        recs = OPEN + LATER_PAIR
    else:
        recs = _scenarios()[name]
    world.put(*recs)
    qs = [q for t in undecided()["trips"] for q in t["questions"] if q["question_id"] == qid]
    assert qs, f"the engine produced no trip with {name!r} for this scenario"
    for q in qs:
        assert list(q) == contract.QUESTION_FIELDS
        if qid in contract.QUESTIONS_WITHOUT_TRAVEL_ANSWER:
            assert q["answers"] == [] and "finance" in q["text"]
            continue
        assert contract.QUESTION_ANSWERS_MIN <= len(q["answers"]) <= contract.QUESTION_ANSWERS_MAX
        assert len({a["answer_id"] for a in q["answers"]}) == len(q["answers"])
        for a in q["answers"]:
            assert list(a) == contract.ANSWER_FIELDS
            assert a["correction"]["kind"] in contract.CORRECTION_KINDS and a["correction"]["record_ids"]
            assert (a["correction"]["kind"] in ("returned", "left")) == ("date" in a["correction"])


# ------------------------------------------------------------------ clause 2: show, then write through 2E
def test_dry_run_shows_the_correction_and_writes_nothing_then_the_real_call_writes_it(world):
    world.put(*OPEN)
    q = undecided()["trips"][0]["questions"][0]
    assert q["question_id"] == "q_return_not_recorded"
    ans = next(a for a in q["answers"] if a["answer_id"] == "back_day_after")
    assert ans["correction"] == {"kind": "returned", "date": "2024-03-02", "record_ids": ["f1"]}
    args = {"trip_key": "f1", "question_id": q["question_id"], "answer_id": "back_day_after", "session": "s1"}

    dry = call("answer_trip_question", {**args, "dry_run": True})["result"]["resolved_call"]
    assert dry["sideEffects"]["writeCount"] == 1 and dry["underlying"][0]["method"] == "PutItem"
    body = dry["underlying"][0]["body"]
    assert (body["correction_kind"], body["date"], body["record_ids"], body["question_id"]) == (
        "returned", "2024-03-02", ["f1"], q["question_id"])
    assert body["author"] == "io@example.test" and body["session"] == "s1"
    assert dry["diff"][0] == {"pointer": "/last_date", "before": "2024-03-01", "after": "2024-03-02"}
    assert rows(world) == []                                         # a dry run writes nothing

    out = call("answer_trip_question", args)
    assert out["success"] is True, out
    written = rows(world)
    assert len(written) == 1                                          # one row, through lane 2E
    assert out["result"]["correction"]["correction_id"] == written[0]["record_id"]
    assert written[0]["author"] == "io@example.test" and written[0]["question_id"] == "q_return_not_recorded"
    assert written[0]["correction_kind"] == "returned"


def test_the_write_goes_through_lane_2e_not_around_it(world, monkeypatch):
    world.put(*OPEN)
    seen = []
    orig = lane2e.HANDLERS["record_trip_correction"]

    async def spy(args):
        seen.append(dict(args))
        return await orig(args)
    monkeypatch.setitem(lane2e.HANDLERS, "record_trip_correction", spy)
    call("answer_trip_question", {"trip_key": "f1", "question_id": "q_return_not_recorded", "answer_id": "back_on_last_record"})
    assert [s["kind"] for s in seen] == ["returned"] and seen[0]["question_id"] == "q_return_not_recorded"
    assert "author" not in seen[0]                                    # 2E stamps it from the principal


# ------------------------------------------------------------------ clause 3: the trip closes, against the real engine
@pytest.mark.parametrize("records,qid,answer", [
    (OPEN, "q_return_not_recorded", "back_on_last_record"),
    (OPEN, "q_return_not_recorded", "back_day_after"),
    (OPEN, "q_return_not_recorded", "not_a_trip"),
    (ONLY_BACK, "q_start_not_recorded", "left_on_first_record"),
    (ONLY_BACK, "q_start_not_recorded", "left_day_before"),
    (FUTURE_OPEN, "q_return_not_planned_yet", "back_after_last_booking"),
    (FUTURE_OPEN, "q_return_not_planned_yet", "not_going"),
])
def test_choosing_an_answer_closes_the_trip(world, records, qid, answer):
    world.put(*records)
    trip = undecided()["trips"][0]
    out = call("answer_trip_question", {"trip_key": trip["key"], "question_id": qid, "answer_id": answer})
    assert out["success"] is True, out
    assert out["result"]["closed"] is True
    assert undecided()["trips"] == []                                 # recomputed from storage: nothing left to decide
    kind = next(a for a in trip["questions"][0]["answers"] if a["answer_id"] == answer)["correction"]["kind"]
    if kind != "leave_out" and out["result"]["trip"] is None:        # the answer made it a day out and back: no trip
        res = te.compute_trips(list(records), ENGINE_HOME, today=TODAY, home_places=[home_places()],
                               corrections=lane2e.engine_corrections(world.records))
        assert [x["disposition"] for x in res["excluded"]] == ["day_out_and_back"]
    elif kind != "leave_out":                                         # boundary reads recorded, correction listed
        t = out["result"]["trip"]
        side = "last" if kind == "returned" else "first"
        assert t[f"{side}_basis"] == "recorded" and t["corrections"][0]["kind"] == kind
        res = te.compute_trips(list(records), ENGINE_HOME, today=TODAY, home_places=[home_places()],
                               corrections=lane2e.engine_corrections(world.records))
        assert res["unapplied_corrections"] == []
        assert res["trips"][0][f"{side}_basis"] == "recorded" and res["trips"][0]["corrections"][0]["question_id"] == qid


def test_retiring_the_correction_reopens_the_question(world):
    world.put(*OPEN)
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": "q_return_not_recorded", "answer_id": "back_day_after"})
    assert undecided()["trips"] == []
    call("retire_trip_correction", {"correction_id": out["result"]["correction"]["correction_id"]})
    assert [q["question_id"] for q in undecided()["trips"][0]["questions"]] == ["q_return_not_recorded"]


def test_a_duplicate_is_closed_by_leaving_one_out(world):
    world.put(*_scenarios()["likely duplicate"])
    t = undecided()["trips"][0]
    q = next(x for x in t["questions"] if x["question_id"] == "q_likely_duplicate")
    assert [a["answer_id"] for a in q["answers"]] == ["leave_out_later", "leave_out_earlier"]
    out = call("answer_trip_question", {"trip_key": t["key"], "question_id": q["question_id"], "answer_id": "leave_out_later"})
    assert out["success"] is True and out["result"]["closed"] is True
    assert "likely duplicate" not in out["result"]["trip"]["signals"]


@pytest.mark.parametrize("name,answer", [("is this place home", "away_trip_stands"), ("unusually long", "long_is_right")])
def test_a_signal_the_records_keep_raising_closes_by_being_answered(world, name, answer):
    world.put(*_scenarios()[name])
    qid = contract.QUESTIONS[name]
    t = next(t for t in undecided()["trips"] if any(q["question_id"] == qid for q in t["questions"]))
    out = call("answer_trip_question", {"trip_key": t["key"], "question_id": qid, "answer_id": answer})
    assert out["success"] is True and out["result"]["closed"] is True
    assert all(q["question_id"] != qid for tt in undecided()["trips"] for q in tt["questions"])
    again = call("answer_trip_question", {"trip_key": t["key"], "question_id": qid, "answer_id": answer})
    assert again["error_envelope"]["code"] == "duplicate" and len(rows(world)) == 1


def test_home_not_recorded_has_no_answer_to_choose(world):
    lane._HOME_KWARGS["invoker"] = finance([{**PERIOD, "effective_from": "2024-06-01"}])
    world.put(*OPEN, *LATER_PAIR)
    t = next(t for t in undecided()["trips"] if t["key"] == "f1")
    q = next(q for q in t["questions"] if q["question_id"] == "q_home_not_recorded")
    assert q["answers"] == []
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": q["question_id"], "answer_id": "x"})
    assert out["error_envelope"]["code"] == "invalid" and rows(world) == []


# ------------------------------------------------------------------ the two validations
def test_an_unknown_question_id_is_refused_and_nothing_is_written(world):
    world.put(*OPEN)
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": "q_made_up", "answer_id": "back_day_after"})
    assert out["error_envelope"]["code"] == "invalid" and "q_made_up" in out["error"]
    assert rows(world) == []


def test_a_question_that_is_not_open_on_the_trip_is_refused(world):
    world.put(*OPEN)
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": "q_start_not_recorded", "answer_id": "left_day_before"})
    assert out["error_envelope"]["code"] == "missing" and rows(world) == []


def test_an_answer_not_offered_is_refused(world):
    world.put(*OPEN)
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": "q_return_not_recorded", "answer_id": "teleported"})
    assert out["error_envelope"]["code"] == "invalid" and "back_on_last_record" in out["error"] and rows(world) == []


def test_record_ids_naming_something_absent_are_refused_not_written(world):
    world.put(*OPEN)
    with pytest.raises(lane._Fail) as e:
        lane._validate_record_ids(["f1", "no-such-flight", "lodging-0123456789abcdef"])
    assert e.value.code == "missing" and "no-such-flight" in e.value.message and "lodging-0123456789abcdef" in e.value.message
    lane._validate_record_ids(["f1"])                                 # a present record passes


def test_record_ids_that_vanish_between_list_and_answer_are_refused(world, monkeypatch):
    """The answer is built from a fresh read; the check sees what that read did not: the record is gone by write time."""
    world.put(*OPEN)
    real = lane._validate_record_ids

    def after_delete(ids):
        world.flights.delete_item(Key={"flight_id": "f1"})
        return real(ids)
    monkeypatch.setattr(lane, "_validate_record_ids", after_delete)
    out = call("answer_trip_question", {"trip_key": "f1", "question_id": "q_return_not_recorded", "answer_id": "back_day_after"})
    assert out["error_envelope"]["code"] == "missing" and "f1" in out["error"] and rows(world) == []


def test_a_cancelled_record_cannot_be_named(world):
    world.put({**OPEN[0], "status": "cancelled"})
    with pytest.raises(lane._Fail) as e:
        lane._validate_record_ids(["f1"])
    assert e.value.code == "missing"


def test_every_refusal_is_a_contract_error_code(world):
    world.put(*OPEN)
    for args in ({"trip_key": "f1", "question_id": "q_made_up", "answer_id": "a"},
                 {"trip_key": "nope", "question_id": "q_return_not_recorded", "answer_id": "a"},
                 {"trip_key": "f1", "question_id": "q_return_not_recorded"}):
        out = call("answer_trip_question", args)
        assert out["success"] is False and out["error_envelope"]["code"] in contract.ERROR_CODES


def test_the_engine_would_report_what_the_validation_prevents(world):
    """The write refuses an absent record (invalid, dry run too, nothing written); the engine still calls such a correction
    unapplied should one exist (a record removed after the correction was written)."""
    before = rows(world)
    for dry in (True, False):
        out = call("record_trip_correction", {"kind": "returned", "date": "2024-03-06", "record_ids": ["no-such-flight"], "dry_run": dry})
        assert out["success"] is False and out["error_envelope"]["code"] == "invalid" and "no-such-flight" in out["error"]
    assert rows(world) == before
    corr = {"correction_id": "correction-x", "kind": "returned", "date": "2024-03-06", "record_ids": ["no-such-flight"], "status": "active"}
    res = te.compute_trips(OPEN, ENGINE_HOME, today=TODAY, home_places=[home_places()], corrections=[corr])
    assert [u["record_ids"] for u in res["unapplied_corrections"]] == [["no-such-flight"]]


def test_a_stale_trip_key_is_refused_not_written(world):
    """A key is the id of the trip's first record and is recomputed on every read: list, change the key, answer with the old one."""
    world.put(fl("k1", "2024-03-01", _SEA, _JFK), fl("k2", "2024-03-02", _JFK, _BOS))
    trip = undecided()["trips"][0]
    q = next(x for x in trip["questions"] if x["question_id"] == "q_return_not_recorded")
    assert trip["key"] == "k1"
    call("record_trip_correction", {"kind": "leave_out", "record_ids": ["k1"]})          # unrelated to this question, moves the key
    assert [t["key"] for t in undecided()["trips"]] == ["k2"]
    before = rows(world)
    for dry in (True, False):
        out = call("answer_trip_question", {"trip_key": "k1", "question_id": q["question_id"], "answer_id": "back_day_after",
                                            "dry_run": dry})
        assert out["success"] is False and out["error_envelope"]["code"] == "missing" and "k1" in out["error"]
    assert rows(world) == before                                                         # nothing written onto anything
    fresh = undecided()["trips"][0]                                                      # the fresh key answers fine
    assert call("answer_trip_question", {"trip_key": fresh["key"], "question_id": q["question_id"],
                                         "answer_id": "back_day_after"})["success"] is True


# ------------------------------------------------------------------ reads and refusals
def test_list_respects_range_and_limit_and_reports_finance_errors(world):
    world.put(*OPEN, *FUTURE_OPEN)
    assert undecided()["count"] == 2 and len(undecided(limit=1)["trips"]) == 1
    assert [t["key"] for t in undecided(date_to="2024-03-02")["trips"]] == ["f1"]
    assert call("list_undecided_trips", {"limit": 0})["error_envelope"]["code"] == "invalid"
    lane._HOME_KWARGS["invoker"] = finance([])
    assert call("list_undecided_trips", {})["error_envelope"]["code"] == "NO_HOME_RECORDED"
    lane._HOME_KWARGS["invoker"] = lambda *a: (_ for _ in ()).throw(TimeoutError("t"))
    assert call("list_undecided_trips", {})["error_envelope"]["code"] == "HOME_UNAVAILABLE"


def test_a_trip_with_no_open_question_is_not_listed(world):
    world.put(fl("a1", "2024-03-01", _SEA, _JFK), fl("a2", "2024-03-02", _JFK, _SEA))
    assert undecided() == {"trips": [], "count": 0}


def test_handlers_are_wired_not_stubbed():
    assert set(lane.HANDLERS) == {"list_undecided_trips", "answer_trip_question"}
    assert {t for t, l in contract.TOOL_LANES.items() if l == "3B"} == set(lane.HANDLERS)
    assert lf._TOOL_HANDLERS["answer_trip_question"] is lane.answer_trip_question
