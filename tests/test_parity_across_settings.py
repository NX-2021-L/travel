"""FLY-TSK-031-0A, A-14: the tool list and schemas are the build's, and the same under any settings.

One artifact runs every deployment (objective O-6). This is the test that fails if any setting, region,
surface name, door or allow-list reaches the tool list, the schemas, caps.json or permission_manifest.json.
Each case runs in a FRESH process whose environment is exactly one deployment's settings, so nothing
leaks in from conftest or from another case. Needs the lambda's dependencies (CI with the private package).
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("io_kit_mcp_edge")

ROOT = Path(__file__).resolve().parents[1]
TL = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(TL))
from load_deployment_settings import parse  # noqa: E402

# io's deployment: the very file the workflows deploy (a test fixture here, so it may name io).
IO = parse((ROOT / "deploy" / "settings.env").read_text())
IO = {k: v for k, v in IO.items() if k in {
    "TABLE_NAME", "RECORDS_TABLE_NAME", "SURFACE_NAME", "FINANCE_READ_FUNCTION", "COGNITO_REGION",
    "PUBLIC_BASE_URL", "CORS_ORIGINS", "DOOR_MODE", "AWS_REGION"}}
IO.update({"COGNITO_USER_POOL_ID": "pool-io", "COGNITO_CLIENT_ID": "client-io", "COGNITO_ALLOWED_SUBS": "sub-io"})

# A synthetic second deployment that differs in EVERY setting: other names, other region, internal door,
# write groups, a second issuer's allow-list, a credential, no public URL and no origins.
NANA = {
    "TABLE_NAME": "nana-flights-x", "RECORDS_TABLE_NAME": "nana-records-x", "SURFACE_NAME": "nana-travel-x",
    "FINANCE_READ_FUNCTION": "https://finance.nana.invalid/", "FINANCE_READ_CREDENTIAL": "nana-credential",
    "COGNITO_USER_POOL_ID": "eu-central-1_NANAPOOL", "COGNITO_CLIENT_ID": "nana-client",
    "COGNITO_REGION": "eu-central-1", "COGNITO_ALLOWED_SUBS": "nana-a,nana-b", "IOGRAPH_ALLOWED_SUBS": "nana-c",
    "TRAVEL_WRITE_GROUPS": "nana-writers,nana-admins", "DOOR_MODE": "internal",
    "INTERNAL_INVOKER_ROLE": "arn:aws:iam::000000000000:role/nana-invoker", "AWS_REGION": "eu-central-1",
}
# A third that differs again, with the AWS default-region spelling a runtime may use.
THIRD = {**NANA, "SURFACE_NAME": "third", "DOOR_MODE": "public", "PUBLIC_BASE_URL": "https://third.invalid",
         "CORS_ORIGINS": "https://a.invalid,https://b.invalid", "AWS_REGION": "ap-south-1", "TRAVEL_WRITE_GROUPS": "",
         "TABLE_NAME": "third-flights", "RECORDS_TABLE_NAME": "third-records", "COGNITO_REGION": "ap-south-1"}
# FLY-TSK-043: an internal deployment that supplies NONE of the five (sign-in pool, client, region, allow-list, finance).
_FIVE = {"COGNITO_USER_POOL_ID", "COGNITO_CLIENT_ID", "COGNITO_REGION", "COGNITO_ALLOWED_SUBS", "FINANCE_READ_FUNCTION"}
BARE = {k: v for k, v in NANA.items() if k not in _FIVE}
BARE.update({"AWS_REGION": "sa-east-1", "SURFACE_NAME": "bare-x", "TABLE_NAME": "bare-flights", "RECORDS_TABLE_NAME": "bare-records"})
SETTINGS_SETS = {"io": IO, "nana": NANA, "third": THIRD, "bare": BARE}

_GENERATE = """
import asyncio, json, sys
sys.path.insert(0, sys.argv[1])
import lambda_function as lf
def r(d):
    return json.dumps(d, indent=2, sort_keys=True) + "\\n"
tools = [t.model_dump(mode="json", exclude_none=True) for t in asyncio.run(lf.list_tools())]
print(json.dumps({
    "caps": r(lf.build_caps()), "manifest": r(lf.build_permission_manifest()), "tools": r(tools),
    "served_caps": lf.served_caps(), "served_manifest": lf.served_permission_manifest(),
}))
"""


def _generate(settings):
    keep = {k: os.environ[k] for k in ("PATH", "PYTHONPATH", "HOME", "SYSTEMROOT") if k in os.environ}
    run = subprocess.run([sys.executable, "-c", _GENERATE, str(TL)], capture_output=True, text=True,
                         env={**keep, **settings}, cwd=str(ROOT))
    assert run.returncode == 0, run.stderr[-2000:]
    return json.loads(run.stdout)


@pytest.fixture(scope="module")
def built():
    return {name: _generate(s) for name, s in SETTINGS_SETS.items()}


def test_the_settings_sets_really_differ():
    for key in ("SURFACE_NAME", "TABLE_NAME", "AWS_REGION", "COGNITO_REGION"):
        assert len({s.get(key) for s in SETTINGS_SETS.values()}) == len(SETTINGS_SETS), key
    assert {s["DOOR_MODE"] for s in SETTINGS_SETS.values()} == {"public", "internal"}


@pytest.mark.parametrize("what", ["caps", "manifest", "tools"])
def test_output_is_byte_identical_under_every_settings_set(built, what):
    outs = {name: b[what] for name, b in built.items()}
    assert len({o.encode() for o in outs.values()}) == 1, f"{what} differs between settings sets: {sorted(outs)}"
    assert len(next(iter(outs.values()))) > 1000  # not trivially equal because empty


def test_output_equals_the_committed_build_byte_for_byte(built):
    """The committed snapshots are the build; a deployment under ANY settings generates exactly them."""
    for name, b in built.items():
        assert b["caps"] == (TL / "caps.json").read_text(), name
        assert b["manifest"] == (TL / "permission_manifest.json").read_text(), name


def test_no_setting_value_appears_in_the_build_output(built):
    """Nothing a deployment supplies is in the bytes: not a name, a region, a pool, a group or an origin."""
    blob = built["io"]["caps"] + built["io"]["manifest"] + built["io"]["tools"]
    values = {v for s in SETTINGS_SETS.values() for k, v in s.items() if len(v) > 3 and k != "DOOR_MODE"}  # a mode word is vocabulary, not an instance value
    leaked = sorted(v for v in values if v in blob)
    assert not leaked, f"setting values appear in the build output: {leaked}"


def test_served_documents_add_only_the_deployments_own_fields(built):
    for name, b in built.items():
        s = SETTINGS_SETS[name]
        caps, build = b["served_caps"], json.loads(b["caps"])
        assert caps["surface"] == s["SURFACE_NAME"] and caps["generatedAt"]
        assert {k: v for k, v in caps.items() if k not in ("surface", "generatedAt")} == build, name
        # the tool list and schemas a deployment serves ARE the build's, byte for byte
        assert json.dumps(caps["actions"], sort_keys=True) == json.dumps(build["actions"], sort_keys=True)
        man, mbuild = b["served_manifest"], json.loads(b["manifest"])
        assert man["surface"] == s["SURFACE_NAME"]
        assert man["principals"]["cognito"]["write_groups"] == [g for g in s.get("TRAVEL_WRITE_GROUPS", "").split(",") if g]
        man["principals"]["cognito"].pop("write_groups")
        man.pop("surface")
        assert man == mbuild, name


def test_mcp_tools_list_and_caps_name_the_same_tools_and_schemas(built):
    """tools/list and /caps.json come from one source: same names, same input schemas."""
    tools = json.loads(built["io"]["tools"])
    actions = json.loads(built["io"]["caps"])["actions"]
    assert [t["name"] for t in tools] == [a["name"] for a in actions]
    assert [t["inputSchema"] for t in tools] == [a["inputSchema"] for a in actions]
    import contract
    assert [a["name"] for a in actions] == list(contract.TOOL_NAMES)


def test_the_committed_snapshots_name_no_deployment():
    for n in ("caps.json", "permission_manifest.json"):
        doc = json.loads((TL / n).read_text())
        assert "surface" not in doc and "generatedAt" not in doc, n
