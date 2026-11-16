"""A-7 (FLY-TSK-029-0B): home places set and read per residence; a near-home unlisted place raises the question. Offline (moto)."""
import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "x")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "x")
D = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
pytest.importorskip("mcp")
moto = pytest.importorskip("moto")
import boto3  # noqa: E402
import contract as c  # noqa: E402
import lane_2b_home as h  # noqa: E402

SEA = {"place_id": "apt:SEA", "area_id": "city:seattle-wa-us", "label": "Sea-Tac", "lat": 47.45, "lon": -122.31, "radius_km": 60}


def run(coro):
    return json.loads(asyncio.run(coro)[0].text)


@pytest.fixture
def table(monkeypatch):
    with moto.mock_aws():
        db = boto3.resource("dynamodb", region_name="us-west-2")
        db.create_table(TableName="rt", BillingMode="PAY_PER_REQUEST",
                        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
        monkeypatch.setenv("RECORDS_TABLE_NAME", "rt")
        monkeypatch.setattr(h, "_resource", None)
        yield db.Table("rt")


def _set(uid, places, **kw):
    return run(h.HANDLERS["set_home_places"]({"residence_uid": uid, "places": places, **kw}))


def test_handlers_cover_both_tools():
    assert set(h.HANDLERS) == {"get_home_places", "set_home_places"} == {n for n, l in c.TOOL_LANES.items() if l == "2B"}


def test_set_and_read_per_residence(table):
    r = _set("res-1", [SEA, {"place_id": "city:seattle-wa-us"}], valid_from="2020-01-01")
    assert r["success"] and r["result"]["home_places"]["pk"] == c.record_keys("home_places", ["res-1"])["pk"]
    assert {p["area_id"] for p in r["result"]["home_places"]["places"]} == {"city:seattle-wa-us"}
    _set("res-2", [{"place_id": "apt:PDX", "area_id": "city:portland-or-us"}])
    one = run(h.HANDLERS["get_home_places"]({"residence_uid": "res-1"}))["result"]
    assert one["count"] == 1 and one["home_places"][0]["residence_uid"] == "res-1"
    assert one["home_places"][0]["places"][0]["radius_km"] == 60
    assert run(h.HANDLERS["get_home_places"]({}))["result"]["count"] == 2
    assert run(h.HANDLERS["get_home_places"]({"residence_uid": "nope"}))["result"]["count"] == 0
    again = _set("res-1", [{"place_id": "city:seattle-wa-us"}])  # one record per residence: replaced, not duplicated
    assert again["result"]["replaced"] and len(table.scan()["Items"]) == 2


def test_validation_and_dry_run(table):
    assert _set("r", [{"place_id": "SEA"}])["error_envelope"]["code"] == "invalid"
    assert _set("r", [{"place_id": "apt:SEA"}])["error_envelope"]["code"] == "invalid"  # no area_id for an airport
    assert _set("r", [dict(SEA, lat=None)])["error_envelope"]["code"] == "invalid"  # radius needs coordinates
    assert _set("r", [SEA], valid_from="2021-01-01", valid_to="2020-01-01")["error_envelope"]["code"] == "invalid"
    assert _set("r", [SEA], dry_run=True)["result"]["dry_run"] and not table.scan()["Items"]


def _homes(table):
    _set("res-1", [SEA], valid_from="2020-01-01", valid_to="2022-12-31")
    return run(h.HANDLERS["get_home_places"]({}))["result"]["home_places"]


def test_place_in_home_area_raises_nothing(table):
    homes = _homes(table)
    stn = {"place_id": "stn:SEA", "area_id": "city:seattle-wa-us", "lat": 47.6, "lon": -122.33}
    assert h.evaluate_place(homes, stn, "2021-05-01")["status"] == "home"
    assert h.evaluate_place(homes, {"place_id": "apt:SEA", "area_id": "city:seattle-wa-us"}, "2021-05-01")["question"] is None


def test_near_home_unlisted_raises_the_contract_question(table):
    homes = _homes(table)
    tacoma = {"place_id": "city:tacoma-wa-us", "area_id": "city:tacoma-wa-us", "label": "Tacoma", "lat": 47.25, "lon": -122.44}
    out = h.evaluate_place(homes, tacoma, "2021-05-01", trip_key="trip-1")
    q = out["question"]
    assert out["status"] == "ask" and q["question_id"] == c.QUESTIONS["is this place home"] == "q_is_this_place_home"
    assert list(q) == c.QUESTION_FIELDS and q["trip_key"] == "trip-1"
    assert c.QUESTION_ANSWERS_MIN <= len(q["answers"]) <= c.QUESTION_ANSWERS_MAX
    assert all(list(a) == c.ANSWER_FIELDS for a in q["answers"])
    assert q["question_id"] not in c.QUESTIONS_WITHOUT_TRAVEL_ANSWER
    # far away, outside the dates, or without a radius: no question
    assert h.evaluate_place(homes, dict(tacoma, lat=45.5, lon=-122.6), "2021-05-01")["status"] == "not_home"
    assert h.evaluate_place(homes, tacoma, "2024-01-01")["status"] == "not_home"
    _set("res-1", [dict(SEA, radius_km=None)])
    nr = run(h.HANDLERS["get_home_places"]({}))["result"]["home_places"]
    assert h.evaluate_place(nr, tacoma)["status"] == "not_home"


def test_missing_setting_is_a_structured_error(monkeypatch):
    monkeypatch.delenv("RECORDS_TABLE_NAME", raising=False)
    for tool, a in (("get_home_places", {}), ("set_home_places", {"residence_uid": "r", "places": [SEA]})):
        e = run(h.HANDLERS[tool](a))["error_envelope"]
        assert e["code"] in c.ERROR_CODES and "RECORDS_TABLE_NAME" in e["message"]
