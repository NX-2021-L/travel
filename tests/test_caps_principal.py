"""DVP-TSK-889: caps.json, annotations, x-io-minted, Cognito principal policy."""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import lambda_function as lf  # noqa: E402


def test_caps_has_five_actions_with_annotations():
    caps = lf.build_caps()
    assert caps["schemaVersion"] == "1.1.0" and caps["surface"] == "io-travel" and caps["generatedAt"]
    assert len(caps["actions"]) == 5
    for a in caps["actions"]:
        assert set(a) == {"name", "title", "inputSchema", "outputSchema", "annotations",
                          "requiresGovernanceHash", "via", "dryRun"}
        assert set(a["annotations"]) == {"readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"}
        assert a["via"] == "top-level" and isinstance(a["dryRun"], bool)
    minted = {a["name"] for a in caps["actions"]
              if any(p.get("x-io-minted") for p in a["inputSchema"]["properties"].values())}
    assert minted == {"create_flight"}


def test_caps_validates_against_parity_schema():
    schema = Path(os.environ.get("CAPS_SCHEMA", ""))
    if not schema.is_file():
        pytest.skip("parity caps.schema not available")
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.Draft202012Validator(json.loads(schema.read_text())).validate(json.loads(json.dumps(lf.build_caps())))


def test_tools_list_carries_annotations_and_minted():
    tools = asyncio.run(lf.list_tools())
    assert all(t.annotations is not None for t in tools)
    create = next(t for t in tools if t.name == "create_flight")
    assert create.meta["x-io-minted"]
    assert next(t for t in tools if t.name == "get_flight").annotations.readOnlyHint is True


def test_snapshots_match_code():
    for name, fn in (("caps.json", lf.build_caps), ("permission_manifest.json", lf.build_permission_manifest)):
        strip = lambda d: {k: v for k, v in d.items() if k != "generatedAt"}  # noqa: E731
        assert strip(json.loads((D / name).read_text())) == strip(json.loads(json.dumps(fn()))), name
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


def test_allow_listed_sub_reads_and_writes(monkeypatch):
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", ["s1"])
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", [])
    p = lf._principal_from_claims({"sub": "s1", "email": "io@example.com"})
    assert lf._authorize(p, "get_flight") is None
    assert lf._authorize(p, "create_flight") is None


def test_non_listed_pool_user_denied_read_and_write(monkeypatch):
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", ["s1"])
    p = lf._principal_from_claims({"sub": "coach9", "cognito:groups": ["io-travel-writers"]})
    assert lf._authorize(p, "get_flight") and lf._authorize(p, "cancel_flight")
    assert _call(p, "get_flight", {"flight_id": "x"})["error_envelope"]["code"] == "forbidden"
    assert _call(p, "cancel_flight", {"flight_id": "x"})["error_envelope"]["code"] == "forbidden"


def test_unset_allow_list_denies_cognito(monkeypatch):
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", [])
    p = lf._principal_from_claims({"sub": "s1"})
    assert lf._authorize(p, "get_flight") and lf._authorize(p, "create_flight")


def test_write_groups_extra_requirement(monkeypatch):
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", ["s1"])
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", ["w"])
    assert lf._authorize(lf._principal_from_claims({"sub": "s1"}), "get_flight") is None
    assert lf._authorize(lf._principal_from_claims({"sub": "s1"}), "create_flight")
    assert lf._authorize(lf._principal_from_claims({"sub": "s1", "cognito:groups": ["w"]}), "create_flight") is None
    assert lf._authorize(dict(lf.INTERNAL_PRINCIPAL), "create_flight") is None
    assert lf._authorize(None, "get_flight")


def _edge_event(path, sub, method="GET", pool=None):
    """The event shape the io-kit MCP edge hands the inner handler after validating the bearer."""
    ctx = {"principal": {"type": "human", "sub": sub, "groups": []}, "token_use": "access"}
    if pool:
        ctx["pool"] = pool
    return {"requestContext": {"http": {"method": method, "path": path}}, "headers": {}, "io_kit_mcp": ctx}


def test_http_403_for_non_listed_edge_principal(monkeypatch):
    monkeypatch.setenv("IOGRAPH_ALLOWED_SUBS", "s1")
    for path in ("/mcp", "/caps.json"):
        assert asyncio.run(lf._handle_lambda_event(_edge_event(path, "coach9")))["statusCode"] == 403
    assert asyncio.run(lf._handle_lambda_event(_edge_event("/caps.json", "s1")))["statusCode"] == 200


def test_inner_handler_without_edge_principal_is_401():
    ev = {"requestContext": {"http": {"method": "GET", "path": "/caps.json"}}, "headers": {"authorization": "Bearer x"}}
    assert asyncio.run(lf._handle_lambda_event(ev))["statusCode"] == 401


def test_caps_route_served(monkeypatch):
    monkeypatch.setenv("IOGRAPH_ALLOWED_SUBS", "s1")
    ev = _edge_event("/caps.json", "s1")
    resp = asyncio.run(lf._handle_lambda_event(ev))
    assert resp["statusCode"] == 200 and len(json.loads(resp["body"])["actions"]) == 5
    ev["requestContext"]["http"]["path"] = "/permission-manifest.json"
    assert json.loads(asyncio.run(lf._handle_lambda_event(ev))["body"])["tools"]["create_flight"]["access"] == "write"
