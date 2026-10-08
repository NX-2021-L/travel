"""Regression tests for FLY-ISS-001 -- search_flights pagination and filtering.

The tests run the real ``_search_flights`` handler against a moto-backed
DynamoDB table that mirrors production (hash-only primary key ``flight_id`` and
the three GSIs created in FLY-TSK-004).  moto reproduces DynamoDB's rule that
``Limit`` bounds the items *evaluated*, before ``FilterExpression`` -- the
behaviour at the heart of FLY-ISS-001 -- so the page-fill tests fail on the
single-call implementation and pass once the handler keeps reading.
"""

import asyncio
import json
import os
import random
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ["TABLE_NAME"] = "io-travel-flights"

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"))
import lambda_function as lf  # noqa: E402

TARGET_NEAR = "965bb5b7d9db"  # SFO->PSP 2026-12-10T18:47:00Z, active (early in scan order)
TARGET_FAR = "1b2c3773ee90"   # PSP->SFO 2026-12-13T07:00:00Z, active (late in scan order)

AIRPORTS = ["SFO", "SEA", "LAX", "JFK", "ATL", "PDX", "PHX", "DFW", "ORD", "PSP"]
CITIES = [None, "Seattle", "NYC", "Atlanta", "Palm Springs", "Phoenix", "Dallas"]


def _item(flight_id, date_iso, origin, dest, status="active", trip_city=None, airline=None, cost_type="Airfare"):
    item = {
        "flight_id": flight_id,
        "date_iso": date_iso,
        "date_yyyy_mm": date_iso[:7],
        "status": status,
        "cost_type": cost_type,
    }
    if origin:
        item["origin"] = origin
    if dest:
        item["dest"] = dest
    if trip_city:
        item["trip_city"] = trip_city
    if airline:
        item["airline"] = airline
    return item


def _seed_items():
    """~320 rows, like production; the two upcoming active rows sit far apart."""
    rng = random.Random(1001)
    filler = []
    for i in range(318):
        origin, dest = rng.sample(AIRPORTS, 2)
        filler.append(_item(
            f"f{i:03d}",
            f"{rng.randint(2022, 2026)}-{rng.randint(1, 9):02d}-{rng.randint(1, 28):02d}T00:00:00Z",
            origin, dest,
            status=rng.choice(["active", "active", "active", "cancelled"]),
            trip_city=rng.choice(CITIES),
            airline=rng.choice([None, "Alaska", "Partner"]),
        ))
    near = _item(TARGET_NEAR, "2026-12-10T18:47:00Z", "SFO", "PSP", trip_city="Palm Springs", airline="Alaska")
    far = _item(TARGET_FAR, "2026-12-13T07:00:00Z", "PSP", "SFO", trip_city="Palm Springs", airline="Alaska")
    cancelled_a = _item("c0000000a001", "2026-10-13T00:00:00Z", "SEA", "ATL", status="cancelled", airline="Alaska")
    cancelled_b = _item("c0000000a002", "2026-10-14T00:00:00Z", "SEA", "SFO", status="cancelled", airline="Alaska")
    hotel = _item("h0000000a001", "2026-12-11T00:00:00Z", None, None, trip_city="Palm Springs", cost_type="Hotel")
    # Scan order is insertion order in moto: near target early, far target late.
    return filler[:10] + [near, cancelled_a] + filler[10:240] + [cancelled_b, hotel, far] + filler[240:]


@pytest.fixture
def table():
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-west-2")
        gsi = lambda name, hash_key, range_key: {  # noqa: E731
            "IndexName": name,
            "KeySchema": [{"AttributeName": hash_key, "KeyType": "HASH"},
                          {"AttributeName": range_key, "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }
        tbl = ddb.create_table(
            TableName="io-travel-flights",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"} for a in
                                  ("flight_id", "date_yyyy_mm", "date_iso", "origin", "dest", "trip_city")],
            GlobalSecondaryIndexes=[
                gsi("date-index", "date_yyyy_mm", "date_iso"),
                gsi("route-index", "origin", "dest"),
                gsi("trip-city-index", "trip_city", "date_iso"),
            ],
        )
        for item in _seed_items():
            tbl.put_item(Item=item)
        lf._dynamo_resource = None  # bind the handler to this mocked resource
        yield tbl
        lf._dynamo_resource = None


def search(**args):
    out = asyncio.run(lf._search_flights(args))
    body = json.loads(out[0].text)
    assert body["success"], body
    return body["result"]


def page_all(**args):
    """Follow next_token to the end; returns the list of result pages."""
    pages = []
    token = None
    for _ in range(2000):
        call = dict(args)
        if token:
            call["next_token"] = token
        page = search(**call)
        pages.append(page)
        token = page.get("next_token")
        if not token:
            return pages
    raise AssertionError("pagination did not terminate")


def ids(pages):
    return [f["flight_id"] for p in pages for f in p["flights"]]


def truth(table, predicate):
    """Brute-force ground truth straight from the table."""
    items, kwargs = [], {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            return {i["flight_id"] for i in items if predicate(i)}
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


class SpyTable:
    """Delegates to the real table and records (operation, IndexName) per call."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = []

    def query(self, **kwargs):
        self.calls.append(("query", kwargs.get("IndexName")))
        return self._inner.query(**kwargs)

    def scan(self, **kwargs):
        self.calls.append(("scan", kwargs.get("IndexName")))
        return self._inner.scan(**kwargs)


# ---------------------------------------------------------------------------
# FLY-ISS-001 as reported
# ---------------------------------------------------------------------------

def test_date_from_status_returns_every_match_on_the_first_page(table):
    result = search(date_from="2026-10-08", status="active")
    assert {f["flight_id"] for f in result["flights"]} == {TARGET_NEAR, TARGET_FAR, "h0000000a001"}
    assert "next_token" not in result  # exhausted: nothing left to follow


def test_filtered_pages_are_full_whenever_a_cursor_is_returned(table):
    pages = page_all(date_from="2026-10-08", status="active", limit=1)
    assert all(len(p["flights"]) == 1 for p in pages[:-1])
    assert sorted(ids(pages)) == sorted({TARGET_NEAR, TARGET_FAR, "h0000000a001"})


# ---------------------------------------------------------------------------
# Cursor correctness: paging yields exactly the matching set, once each
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("limit", [1, 3, 7, 50])
@pytest.mark.parametrize("args, predicate", [
    ({"status": "active"}, lambda i: i["status"] == "active"),
    ({"airline": "Alaska"}, lambda i: i.get("airline") == "Alaska"),
    ({"status": "cancelled", "date_from": "2026-01-01"},
     lambda i: i["status"] == "cancelled" and i["date_iso"] >= "2026-01-01"),
    ({"origin": "SFO", "status": "active"},
     lambda i: i.get("origin") == "SFO" and i["status"] == "active"),
    ({"origin": "SFO", "dest": "SEA", "airline": "Alaska"},
     lambda i: i.get("origin") == "SFO" and i.get("dest") == "SEA" and i.get("airline") == "Alaska"),
    ({"trip_city": "Palm Springs", "status": "active"},
     lambda i: i.get("trip_city") == "Palm Springs" and i["status"] == "active"),
    ({}, lambda i: True),
])
def test_paging_returns_exactly_the_matching_rows_once(table, args, predicate, limit):
    pages = page_all(limit=limit, **args)
    found = ids(pages)
    assert len(found) == len(set(found)), "duplicate rows across pages"
    assert set(found) == truth(table, predicate)


def test_correctness_does_not_depend_on_the_read_budget(table, monkeypatch):
    """A tiny budget may return short pages, but the cursor must never skip rows."""
    monkeypatch.setattr(lf, "SEARCH_MAX_DDB_CALLS", 1, raising=False)
    monkeypatch.setattr(lf, "SEARCH_EVAL_CHUNK", 7, raising=False)
    for args, predicate in [
        ({"status": "active", "date_from": "2026-10-08"},
         lambda i: i["status"] == "active" and i["date_iso"] >= "2026-10-08"),
        ({"trip_city": "Palm Springs", "date_from": "2026-06-01"},
         lambda i: i.get("trip_city") == "Palm Springs" and i["date_iso"] >= "2026-06-01"),
    ]:
        pages = page_all(limit=2, **args)
        found = ids(pages)
        assert len(found) == len(set(found))
        assert set(found) == truth(table, predicate)


# ---------------------------------------------------------------------------
# Routing: trip_city + dates must use trip-city-index key conditions
# ---------------------------------------------------------------------------

def test_trip_city_with_dates_queries_trip_city_index(table, monkeypatch):
    spy = SpyTable(table)
    monkeypatch.setattr(lf, "_get_table", lambda: spy)
    result = search(trip_city="Palm Springs", date_from="2026-12-11")
    assert [f["flight_id"] for f in result["flights"]] == ["h0000000a001", TARGET_FAR]
    assert spy.calls and all(call == ("query", "trip-city-index") for call in spy.calls)


def test_trip_city_date_range_is_inclusive_on_both_ends(table):
    result = search(trip_city="Palm Springs", date_from="2026-12-10", date_to="2026-12-13")
    assert {f["flight_id"] for f in result["flights"]} == {TARGET_NEAR, "h0000000a001", TARGET_FAR}


# ---------------------------------------------------------------------------
# date_to: a date-only upper bound covers the whole day
# ---------------------------------------------------------------------------

def test_date_only_date_to_includes_that_whole_day(table):
    result = search(date_from="2026-12-10", date_to="2026-12-10")
    assert [f["flight_id"] for f in result["flights"]] == [TARGET_NEAR]


def test_date_only_date_to_does_not_leak_into_the_next_day(table):
    result = search(date_from="2026-12-10", date_to="2026-12-12")
    assert TARGET_FAR not in {f["flight_id"] for f in result["flights"]}  # 12-13T07:00Z


def test_full_timestamp_date_to_is_unchanged(table):
    assert search(date_from="2026-12-10", date_to="2026-12-10T18:46:59Z")["flights"] == []
    exact = search(date_from="2026-12-10", date_to="2026-12-10T18:47:00Z")
    assert [f["flight_id"] for f in exact["flights"]] == [TARGET_NEAR]


@pytest.mark.parametrize("value, expected", [
    ("2026-12-10", "2026-12-10T23:59:59Z"),
    ("2026-12-10T18:47:00Z", "2026-12-10T18:47:00Z"),
])
def test_date_upper_bound_helper(value, expected):
    assert lf._date_upper_bound(value) == expected


# ---------------------------------------------------------------------------
# Filters that are not index keys must still apply (origin / dest were dropped)
# ---------------------------------------------------------------------------

def test_dest_alone_filters_results(table):
    pages = page_all(dest="PSP")
    assert pages and all(f["dest"] == "PSP" for p in pages for f in p["flights"])
    assert set(ids(pages)) == truth(table, lambda i: i.get("dest") == "PSP")


def test_origin_with_trip_city_filters_results(table):
    pages = page_all(trip_city="Palm Springs", origin="PSP")
    assert set(ids(pages)) == truth(
        table, lambda i: i.get("trip_city") == "Palm Springs" and i.get("origin") == "PSP")
    assert all(f["origin"] == "PSP" for p in pages for f in p["flights"])


def test_dest_with_trip_city_filters_results(table):
    pages = page_all(trip_city="Palm Springs", dest="SFO")
    assert set(ids(pages)) == truth(
        table, lambda i: i.get("trip_city") == "Palm Springs" and i.get("dest") == "SFO")


# ---------------------------------------------------------------------------
# Unchanged behaviour
# ---------------------------------------------------------------------------

def test_trip_city_alone_still_lists_in_date_order(table):
    result = search(trip_city="Palm Springs")
    dates = [f["date_iso"] for f in result["flights"]]
    assert dates == sorted(dates)
    assert TARGET_NEAR in {f["flight_id"] for f in result["flights"]}


def test_limit_is_clamped_to_a_positive_page_size(table):
    assert len(search(limit=500)["flights"]) <= 50
    assert len(search(limit=0)["flights"]) == 1


# ---------------------------------------------------------------------------
# Cursor validation: stale or foreign tokens fail cleanly
# ---------------------------------------------------------------------------

def _raw_search(**args):
    return json.loads(asyncio.run(lf._search_flights(args))[0].text)


@pytest.mark.parametrize("token", [
    "%%%not-base64%%%",
    "bm90IGpzb24=",           # base64("not json")
    "WzEsIDJd",               # base64("[1, 2]") -- JSON, but not a cursor object
])
def test_malformed_next_token_is_rejected_cleanly(table, token):
    body = _raw_search(status="active", next_token=token)
    assert body["success"] is False
    assert body["error_envelope"]["code"] == "invalid_next_token"


class _RejectingTable:
    """Raises what DynamoDB raises for a starting key from a different access path."""

    def __init__(self, code):
        self._code = code

    def _fail(self, **_kwargs):
        from botocore.exceptions import ClientError
        raise ClientError({"Error": {"Code": self._code, "Message": "The provided starting key is invalid"}}, "Query")

    query = scan = _fail


def test_cursor_from_another_access_path_is_reported_not_raised(monkeypatch):
    # e.g. a pre-fix scan cursor replayed against the trip-city-index query
    monkeypatch.setattr(lf, "_get_table", lambda: _RejectingTable("ValidationException"))
    cursor = lf.b64.b64encode(json.dumps({"flight_id": "f001"}).encode()).decode()
    body = _raw_search(trip_city="Palm Springs", next_token=cursor)
    assert body["success"] is False
    assert body["error_envelope"]["code"] == "invalid_next_token"


def test_validation_errors_without_a_cursor_are_not_masked(monkeypatch):
    from botocore.exceptions import ClientError
    monkeypatch.setattr(lf, "_get_table", lambda: _RejectingTable("ValidationException"))
    with pytest.raises(ClientError):
        _raw_search(trip_city="Palm Springs")
