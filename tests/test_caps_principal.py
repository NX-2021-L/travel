"""DVP-TSK-889: caps.json, annotations, x-io-minted, Cognito principal policy."""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import lambda_function as lf  # noqa: E402


def test_caps_has_five_actions_with_annotations():
    caps = lf.build_caps()
    assert len(caps["actions"]) == 5
    for a in caps["actions"]:
        assert set(a["annotations"]) == {"readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"}
        assert a["schemaSource"] == "declared" and len(a["schemaHash"]) == 64
    assert {a["action"] for a in caps["actions"] if "x-io-minted" in a} == {"create_flight"}


def test_tools_list_carries_annotations_and_minted():
    tools = asyncio.run(lf.list_tools())
    assert all(t.annotations is not None for t in tools)
    create = next(t for t in tools if t.name == "create_flight")
    assert create.meta["x-io-minted"]
    assert next(t for t in tools if t.name == "get_flight").annotations.readOnlyHint is True


def test_snapshots_match_code():
    for name, fn in (("caps.json", lf.build_caps), ("permission_manifest.json", lf.build_permission_manifest)):
        assert json.loads((D / name).read_text()) == json.loads(json.dumps(fn())), name
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "emit_caps.py"), "--check"], capture_output=True)
    assert r.returncode == 0, r.stdout


def _call(principal, tool, args=None):
    tok = lf._PRINCIPAL.set(principal)
    try:
        async def run():
            return await lf.call_tool(tool, args or {})
        return json.loads(asyncio.run(run())[0].text)
    finally:
        lf._PRINCIPAL.reset(tok)


def test_policy_ungrouped_write_refused_read_allowed(monkeypatch):
    human = lf._principal_from_claims({"sub": "s1", "email": "io@example.com"})
    assert lf._authorize(human, "get_flight") is None
    assert lf._authorize(human, "create_flight")
    out = _call(human, "cancel_flight", {"flight_id": "x"})
    assert out["error_envelope"]["code"] == "forbidden"


def test_policy_grouped_and_internal_write_allowed():
    grouped = lf._principal_from_claims({"sub": "s1", "cognito:groups": ["io-travel-writers"]})
    assert lf._authorize(grouped, "update_flight") is None
    assert lf._authorize(dict(lf.INTERNAL_PRINCIPAL), "create_flight") is None
    # no principal => refused
    assert lf._authorize(None, "get_flight")


def test_authenticate_internal_key_and_cognito(monkeypatch):
    monkeypatch.setattr(lf, "MCP_API_KEY", "k" * 12)
    monkeypatch.setattr(lf, "COGNITO_USER_POOL_ID", "us-east-1_x")
    monkeypatch.setattr(lf, "_verify_cognito_jwt", lambda t: {"sub": "u", "email": "a@b.c", "cognito:groups": ["g"]})
    p, err = lf._authenticate({"headers": {"Authorization": "Bearer " + "k" * 12}})
    assert err is None and p["kind"] == "internal-key"
    p, err = lf._authenticate({"headers": {"Authorization": "Bearer jwt"}})
    assert err is None and p["kind"] == "cognito" and p["groups"] == ["g"]
    assert lf._authenticate({"headers": {}})[0] is None


def test_caps_route_served(monkeypatch):
    monkeypatch.setattr(lf, "MCP_API_KEY", "secretkey")
    ev = {"requestContext": {"http": {"method": "GET", "path": "/caps.json"}},
          "headers": {"authorization": "Bearer secretkey"}}
    resp = asyncio.run(lf._handle_lambda_event(ev))
    assert resp["statusCode"] == 200 and len(json.loads(resp["body"])["actions"]) == 5
    ev["requestContext"]["http"]["path"] = "/permission-manifest.json"
    assert json.loads(asyncio.run(lf._handle_lambda_event(ev))["body"])["tools"]["create_flight"]["access"] == "write"
