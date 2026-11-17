"""FLY-TSK-043: sign-in and finance settings are required on a public edge only.

Criterion 1/A-2 again: DOOR_MODE=public with any one of the five blank refuses to start and stops the deploy.
Criterion 2: a deployment built from the template's OWN defaults, DOOR_MODE=internal, none of the five, starts
and answers the LD-39 health read for both internal roles. io_kit_mcp_edge is stubbed in the child process
only when it is not installed (the health read answers before the edge is touched).
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LAMBDA = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(LAMBDA))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import contract  # noqa: E402
import resolve_env  # noqa: E402
import settings  # noqa: E402
from test_travel_deployment_template import T  # noqa: E402  (the CloudFormation-aware loader's parse)

FIVE = ["COGNITO_USER_POOL_ID", "COGNITO_CLIENT_ID", "COGNITO_REGION", "COGNITO_ALLOWED_SUBS", "FINANCE_READ_FUNCTION"]
SENTINEL = "zz-sentinel-zz"
INVOKER = "arn:aws:iam::000000000000:role/test-invoker"
DEPLOYER = "arn:aws:iam::000000000000:role/test-deployer"


def public_env(**over):
    env = {k: f"{SENTINEL}-{k.lower()}" for k in contract.SETTINGS}
    env.update({"AWS_REGION": "r-1", "DOOR_MODE": "public"}, **over)
    return env


# ---------------------------------------------------------------- the declaration
def test_the_five_are_required_on_the_public_door_only():
    assert {k for k, v in contract.SETTINGS.items() if v["required"] == "public"} == set(FIVE)
    assert len(contract.SETTINGS) == 14


def test_internal_door_requires_none_of_the_five():
    env = public_env(DOOR_MODE="internal")
    for k in FIVE:
        del env[k]
    assert settings.missing_required(env) == []
    assert settings.load(env)["COGNITO_USER_POOL_ID"] == ""


def test_internal_door_still_requires_the_names():
    assert set(settings.missing_required({"DOOR_MODE": "internal"})) == {"TABLE_NAME", "RECORDS_TABLE_NAME", "SURFACE_NAME"}


# ---------------------------------------------------------------- A-2 again, public edge
@pytest.mark.parametrize("blank", ["absent", "empty", "spaces"])
@pytest.mark.parametrize("key", FIVE)
def test_public_edge_refuses_to_start_and_deploy_stops(key, blank):
    env = public_env()
    if blank == "absent":
        del env[key]
    else:
        env[key] = "" if blank == "empty" else "  "
    with pytest.raises(settings.SettingsError) as e:
        settings.load(env)
    assert key in str(e.value) and SENTINEL not in str(e.value)
    assert key in settings.missing_required(env)
    msg = resolve_env.guard(env)
    assert msg.startswith("refusing to deploy") and key in msg


@pytest.mark.parametrize("mode", [None, "", "bogus"])
@pytest.mark.parametrize("key", FIVE)
def test_unknown_or_absent_door_is_read_as_public(key, mode):
    env = public_env()
    del env[key]
    if mode is None:
        del env["DOOR_MODE"]
    else:
        env["DOOR_MODE"] = mode
    assert key in settings.missing_required(env)


def test_public_edge_complete_passes_the_guard():
    assert resolve_env.guard(public_env()) == ""


@pytest.mark.parametrize("key", FIVE)
def test_release_plan_path_stops_a_public_edge_missing_a_sign_in_setting(key):
    # scripts/release_ops.py plan resolves the live env with the supplied settings and runs resolve_env.guard
    live = {k: "v" for k in contract.SETTINGS}
    live["DOOR_MODE"] = "public"
    del live[key]
    assert key in resolve_env.guard(resolve_env.resolve(live, {}))


# ---------------------------------------------------------------- criterion 2: from the template's own defaults
def template_environment():
    """The function's Environment.Variables as the template defines them, parameter defaults resolved."""
    params = T["Parameters"]
    variables = T["Resources"]["McpFunction"]["Properties"]["Environment"]["Variables"]
    env = {}
    for key, val in variables.items():
        if isinstance(val, dict) and "Ref" in val and val["Ref"] in params:
            default = params[val["Ref"]].get("Default")
            # a parameter with no default is the deployment's own value (a role)
            env[key] = default if default is not None else f"supplied-{val['Ref']}"
        elif isinstance(val, dict) and "Ref" in val:  # a resource Ref: the table name CloudFormation resolves
            env[key] = f"resolved-{val['Ref']}"
        elif isinstance(val, str):
            env[key] = val
    return env


def internal_from_template():
    env = template_environment()
    env["DOOR_MODE"] = "internal"           # PublicEdge=false, what the template's IsPublicEdge condition yields
    env["INTERNAL_INVOKER_ROLE"] = INVOKER  # deployment-supplied, not one of the five
    env["INTERNAL_DEPLOY_ROLE"] = DEPLOYER
    env["SURFACE_NAME"] = "template-test-surface"  # not set by the template; the deploy settings supply it
    env["AWS_REGION"] = "r-1"
    return env


def test_template_defaults_supply_none_of_the_five():
    env = template_environment()
    for k in FIVE:
        assert not (env.get(k) or "").strip(), k
    assert env["MCP_TRANSPORT"] == "streamable_http"


def test_template_built_internal_deployment_passes_the_contract():
    env = internal_from_template()
    assert settings.missing_required(env) == []
    assert settings.load(env)["SURFACE_NAME"]
    assert resolve_env.guard(resolve_env.resolve({}, env)) == ""


def test_template_defaults_with_public_door_are_refused():
    env = {**internal_from_template(), "DOOR_MODE": "public"}
    assert set(FIVE) <= set(settings.missing_required(env))


_CHILD = textwrap.dedent("""
    import json, sys, types
    try:
        import io_kit_mcp_edge
    except ImportError:
        m = types.ModuleType("io_kit_mcp_edge")
        for n in ("BearerError", "EdgeConfig", "JwksUnavailable"):
            setattr(m, n, type(n, (Exception,), {}))
        m.validate_bearer = lambda *a, **k: None
        m.wrap = lambda *a, **k: None
        j = types.ModuleType("io_kit_mcp_edge.jwks")
        j.fetch_https = lambda u: {}
        m.jwks = j
        sys.modules.update({"io_kit_mcp_edge": m, "io_kit_mcp_edge.jwks": j})
    sys.path.insert(0, sys.argv[1])
    import lambda_function as lf
    out = {}
    for who in sys.argv[2:]:
        out[who] = lf.lambda_handler({"invoker_role": who, "mcp": {"jsonrpc": "2.0", "id": 7, "method": "ping"}}, None)
    print("RESULT " + json.dumps(out))
""")


def _deps():
    try:
        import anyio, boto3, mcp  # noqa: F401,E401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _deps(), reason="boto3/mcp/anyio not installed (limited CI)")
def test_template_built_internal_deployment_starts_and_answers_health_for_both_roles():
    keep = ("PATH", "HOME", "PYTHONPATH", "VIRTUAL_ENV", "SYSTEMROOT")
    clean = {k: os.environ[k] for k in keep if k in os.environ}
    run = subprocess.run([sys.executable, "-c", _CHILD, str(LAMBDA), INVOKER, DEPLOYER],
                         env={**clean, **internal_from_template()}, capture_output=True, text=True, cwd=str(ROOT))
    assert run.returncode == 0, run.stderr[-1500:]
    got = json.loads(run.stdout.split("RESULT ", 1)[1])
    for who in (INVOKER, DEPLOYER):
        assert got[who]["result"]["status"] == "ok" and got[who]["id"] == 7, who
