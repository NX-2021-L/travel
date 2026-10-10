"""DVP-TSK-900: server-side dry_run on the 3 write tools returns ResolvedCall v1, zero writes."""
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
os.environ["TABLE_NAME"] = "io-travel-flights"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"))
import lambda_function as lf  # noqa: E402


@pytest.fixture
def table():
    with mock_aws():
        lf._dynamo_resource = None
        ddb = boto3.resource("dynamodb", region_name="us-west-2")
        t = ddb.create_table(
            TableName="io-travel-flights",
            KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        t.put_item(Item={"flight_id": "abc", "origin": "SEA", "dest": "SFO", "status": "active", "trip_city": "Old"})
        tok = lf._PRINCIPAL.set(dict(lf.INTERNAL_PRINCIPAL))
        yield t
        lf._PRINCIPAL.reset(tok)
        lf._dynamo_resource = None


def call(tool, args):
    return json.loads(asyncio.run(lf.call_tool(tool, args))[0].text)


def rc(out):
    assert out["success"] is True
    return out["result"]["resolved_call"]


def test_create_dry_run_no_write(table):
    out = call("create_flight", {"date": "2026-06-15", "origin": "sea", "dest": "sfo", "dry_run": True, "idempotency_key": "k1"})
    r = rc(out)
    assert r["v"] == 1 and r["source"] == "server-dry-run" and r["idempotencyKey"] == "k1"
    assert r["sideEffects"] == {"writeCount": 1, "kinds": ["dynamodb:PutItem"]}
    assert {p["pointer"] for p in r["placeholders"]} >= {"/result/flight_id"}
    assert len(r["schemaHash"]) == 64 and r["principal"] == "internal-key"
    assert table.scan()["Count"] == 1


def test_update_dry_run_diff_no_write(table):
    r = rc(call("update_flight", {"flight_id": "abc", "trip_city": "New", "dry_run": True}))
    assert r["diff"] == [{"pointer": "/trip_city", "before": "Old", "after": "New"}]
    assert table.get_item(Key={"flight_id": "abc"})["Item"]["trip_city"] == "Old"


def test_cancel_dry_run_no_write(table):
    r = rc(call("cancel_flight", {"flight_id": "abc", "dry_run": True}))
    assert r["sideEffects"]["writeCount"] == 1 and r["diff"][0]["after"] == "cancelled"
    assert table.get_item(Key={"flight_id": "abc"})["Item"]["status"] == "active"


def test_dry_run_keeps_validation(table):
    assert call("update_flight", {"flight_id": "abc", "date_iso": "x", "dry_run": True})["error_envelope"]["code"] in ("immutable_field", "unknown_field")
    assert call("cancel_flight", {"flight_id": "nope", "dry_run": True})["error_envelope"]["code"] == "not_found"


def test_non_dry_run_still_writes(table):
    call("cancel_flight", {"flight_id": "abc"})
    assert table.get_item(Key={"flight_id": "abc"})["Item"]["status"] == "cancelled"


def test_dry_run_requires_write_authorisation(table):
    lf._PRINCIPAL.set(lf._principal_from_claims({"sub": "s"}))
    assert call("cancel_flight", {"flight_id": "abc", "dry_run": True})["error_envelope"]["code"] == "forbidden"


def test_caps_advertise_dry_run():
    acts = {a["action"]: a for a in lf.build_caps()["actions"]}
    assert all(acts[n]["dry_run"] and "dry_run" in acts[n]["inputSchema"]["properties"] for n in lf.WRITE_TOOLS)
    assert not acts["get_flight"]["dry_run"] and not acts["search_flights"]["dry_run"]
