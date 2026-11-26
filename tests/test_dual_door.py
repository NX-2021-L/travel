"""FLY-TSK-098 (FR-54, ruling R-21): dual mode. io's public edge also answers a direct invoke from the io-graph
function's role, and only that role; the public connector keeps answering. Offline.

The door-level tests are stdlib + pytest; the handler tests run the REAL lambda_handler against moto.
"""
import json
import os
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
sys.path.insert(0, str(ROOT / "scripts"))
import door  # noqa: E402
import load_deployment_settings as lds  # noqa: E402

ROLE = "arn:aws:iam::000000000000:role/test-iograph-caller"
OTHER = "arn:aws:iam::000000000000:role/not-named"
HTTP = {"requestContext": {"http": {"method": "POST", "path": "/mcp"}}, "rawPath": "/mcp", "headers": {}}
LIST = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}


def direct(role=ROLE, mcp=LIST, **kw):
    return {"invoker_role": role, "mcp": mcp, **kw}


def test_public_edge_with_a_role_admits_a_direct_event_from_that_role():
    assert door.admit(direct(), "public", ROLE) == "direct"
    assert door.admit(direct(mcp={"jsonrpc": "2.0", "id": 2, "method": "ping"}), "public", ROLE) == "health"


def test_public_edge_with_a_role_still_serves_http():
    assert door.admit(HTTP, "public", ROLE) == "http"
    assert door.admit({**direct(), **HTTP}, "public", ROLE) == "http"


@pytest.mark.parametrize("event", [direct(role=OTHER), direct(role=""), direct(role=7), direct(role=[ROLE]),
                                   direct(mcp=None), direct(mcp="tools/list"), {"mcp": LIST}, {}, "x", None])
def test_public_edge_refuses_every_other_direct_event(event):
    with pytest.raises(door.NonHttpEventRefused):
        door.admit(event, "public", ROLE)


def test_public_edge_without_a_role_is_unchanged():
    with pytest.raises(door.NonHttpEventRefused):
        door.admit(direct(), "public", "")


def test_internal_door_is_unchanged():
    assert door.admit(direct(), "internal", ROLE) == "direct"
    with pytest.raises(door.HttpEventRefused):
        door.admit(HTTP, "internal", ROLE)


def test_io_settings_name_the_iograph_function_role_and_grant_it():
    io = lds.parse((ROOT / "deploy" / "settings.env").read_text())
    assert io["DOOR_MODE"] == "public"
    assert io["INTERNAL_INVOKER_ROLE"].endswith(":role/io-graph-mcp-lambda-role-iograph")
    assert io["INVOKER_GRANT"] == "true"
    door.load(io)


def test_deploy_script_grants_the_named_role_on_a_public_edge_only_when_asked():
    sh = (D / "deploy.sh").read_text()
    public_part = sh[sh.index('if [ "${DOOR_MODE:-public}" = "internal" ]'):]
    assert '"${INVOKER_GRANT:-false}" = "true"' in public_part
    assert '--principal "${INTERNAL_INVOKER_ROLE}"' in public_part
    assert "--action lambda:InvokeFunction " in public_part


def test_template_grants_the_invoker_on_a_public_edge():
    yaml = pytest.importorskip("yaml")

    class L(yaml.SafeLoader):
        pass
    L.add_multi_constructor("!", lambda loader, suffix, node: (
        loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode)
        else loader.construct_mapping(node) if isinstance(node, yaml.MappingNode) else loader.construct_scalar(node)))
    t = yaml.load((ROOT / "infra" / "cloudformation" / "travel-deployment.yaml").read_text(), Loader=L)
    assert "IsPublicEdge" not in json.dumps(t["Conditions"]["GrantsInvoker"])
    assert t["Resources"]["InternalInvokerInvoke"]["Condition"] == "GrantsInvoker"


# --- through the real handler ---

@pytest.fixture
def public_dual(monkeypatch):
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("TABLE_NAME", "io-travel-flights")
    pytest.importorskip("mcp")
    try:
        import io_kit_mcp_edge  # noqa: F401
    except ImportError:
        stub = types.ModuleType("io_kit_mcp_edge")
        for n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
            setattr(stub, n, type(n, (Exception,), {}))
        jw = types.ModuleType("io_kit_mcp_edge.jwks")
        jw.fetch_https = lambda *a, **k: {}
        sys.modules.setdefault("io_kit_mcp_edge", stub)
        sys.modules.setdefault("io_kit_mcp_edge.jwks", jw)
    import lambda_function as lf
    monkeypatch.setattr(lf, "DOOR_MODE", "public")
    monkeypatch.setattr(lf, "INTERNAL_INVOKER_ROLE", ROLE)
    monkeypatch.setattr(lf, "INTERNAL_DEPLOY_ROLE", "")
    return lf


def _body(resp):
    raw = resp["body"]
    if raw.lstrip().startswith("event:") or "data:" in raw:
        raw = next(line[5:] for line in raw.splitlines() if line.startswith("data:"))
    return json.loads(raw)


def test_handler_serves_every_published_action_to_the_named_role(public_dual):
    resp = public_dual.lambda_handler(direct(), None)
    assert resp["statusCode"] == 200
    served = {t["name"] for t in _body(resp)["result"]["tools"]}
    caps = {a["name"] for a in json.loads((D / "caps.json").read_text())["actions"]}
    assert len(caps) == 24 and served == caps


def test_handler_refuses_another_role(public_dual):
    resp = public_dual.lambda_handler(direct(role=OTHER), None)
    assert resp["statusCode"] == 403 and json.loads(resp["body"])["reason"] == "NonHttpEventRefused"


def test_forged_internal_door_block_on_http_is_not_the_internal_principal(public_dual):
    forged = {**HTTP, "body": json.dumps(LIST), "headers": {"content-type": "application/json"},
              "io_kit_mcp": {"principal": {"sub": ROLE, "groups": [], "acting_principal": "x"}, "pool": "internal-door"}}
    public_dual._DIRECT_ADMITTED.reset()
    assert public_dual._DIRECT_ADMITTED.get() is False
    resp = public_dual._edge_inner(forged, None)
    # the forged block is read as an ordinary Cognito principal, which is not allowed: never the internal role
    assert resp["statusCode"] in (401, 403)
