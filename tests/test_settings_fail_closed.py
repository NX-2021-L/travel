"""FLY-TSK-028-0B, acceptance A-2: with any one setting removed the function refuses to start and
the deploy stops before apply. Offline; no AWS. Parameterised over contract.SETTINGS, so a setting
added later is covered automatically."""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
LAMBDA = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(LAMBDA))
sys.path.insert(0, str(ROOT / "scripts"))
import contract  # noqa: E402
import release_ops  # noqa: E402
import resolve_env  # noqa: E402
import settings  # noqa: E402
import load_deployment_settings as loader  # noqa: E402

REQUIRED = sorted(k for k, v in contract.SETTINGS.items() if v["required"])
OPTIONAL = sorted(k for k, v in contract.SETTINGS.items() if not v["required"])
SENTINEL = "zz-sentinel-value-zz"


def full_env(**over):
    env = {k: f"{SENTINEL}-{k.lower()}" for k in contract.SETTINGS}
    env["AWS_REGION"] = "r-1"
    env.update(over)
    return env


def test_required_set_is_not_empty():
    assert REQUIRED, "contract.SETTINGS declares no required setting"


# ---------------------------------------------------------------- half 1: refuse to start
@pytest.mark.parametrize("key", REQUIRED)
def test_load_refuses_when_one_required_setting_absent(key):
    env = full_env()
    del env[key]
    with pytest.raises(settings.SettingsError) as e:
        settings.load(env)
    assert key in str(e.value)
    assert SENTINEL not in str(e.value), "the refusal must name keys, never values"


@pytest.mark.parametrize("key", REQUIRED)
def test_load_refuses_when_one_required_setting_blank(key):
    with pytest.raises(settings.SettingsError):
        settings.load(full_env(**{key: "  "}))


@pytest.mark.parametrize("key", OPTIONAL)
def test_optional_setting_may_be_absent(key):
    env = full_env()
    del env[key]
    assert settings.load(env)[key] == ""


def test_load_refuses_without_runtime_region():
    env = full_env()
    del env["AWS_REGION"]
    with pytest.raises(settings.SettingsError):
        settings.load(env)


def test_load_returns_every_setting_when_complete():
    got = settings.load(full_env())
    assert set(contract.SETTINGS) <= set(got) and got["TABLE_NAME"].startswith(SENTINEL)


def _import_lambda(env):
    """Import lambda_function in a clean interpreter; io-kit-mcp-edge is stubbed only if it is not installed."""
    code = textwrap.dedent(f"""
        import sys, types
        try:
            import io_kit_mcp_edge
        except ImportError:
            m = types.ModuleType("io_kit_mcp_edge")
            for n in ("BearerError", "EdgeConfig", "JwksUnavailable"):
                setattr(m, n, type(n, (Exception,), {{}}))
            m.validate_bearer = lambda *a, **k: None
            m.wrap = lambda *a, **k: None
            j = types.ModuleType("io_kit_mcp_edge.jwks")
            j.fetch_https = lambda u: {{}}
            m.jwks = j
            sys.modules.update({{"io_kit_mcp_edge": m, "io_kit_mcp_edge.jwks": j}})
        sys.path.insert(0, {str(LAMBDA)!r})
        import lambda_function
        print("STARTED")
    """)
    clean = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
    for k in ("PYTHONPATH", "VIRTUAL_ENV", "SYSTEMROOT"):
        if k in os.environ:
            clean[k] = os.environ[k]
    clean.update(env)
    return subprocess.run([sys.executable, "-c", code], env=clean, capture_output=True, text=True)


def _lambda_deps_present():
    for mod in ("boto3", "mcp", "anyio"):
        try:
            __import__(mod)
        except ImportError:
            return False
    return True


needs_deps = pytest.mark.skipif(not _lambda_deps_present(), reason="boto3/mcp/anyio not installed (limited CI)")


@needs_deps
def test_control_function_starts_with_every_setting():
    r = _import_lambda(full_env())
    assert r.returncode == 0 and "STARTED" in r.stdout, r.stderr[-800:]


@needs_deps
@pytest.mark.parametrize("key", REQUIRED)
def test_function_refuses_to_start_with_one_required_setting_removed(key):
    env = full_env()
    del env[key]
    r = _import_lambda(env)
    assert r.returncode != 0 and "STARTED" not in r.stdout
    assert "SettingsError" in r.stderr and key in r.stderr, r.stderr[-800:]
    assert SENTINEL not in r.stderr


# ---------------------------------------------------------------- half 2: the deploy stops before apply
@pytest.mark.parametrize("key", REQUIRED)
def test_deploy_guard_refuses_when_one_required_setting_absent(key):
    env = full_env()
    del env[key]
    msg = resolve_env.guard(env)
    assert msg.startswith("refusing to deploy") and key in msg and SENTINEL not in msg


def test_deploy_guard_passes_when_complete():
    assert resolve_env.guard(full_env()) == ""


def test_resolve_env_has_no_defaults_for_settings():
    assert resolve_env.resolve({}, {}) == {"MCP_TRANSPORT": "streamable_http"}


def test_resolve_env_supplies_every_contract_setting_and_preserves_live():
    assert set(resolve_env.KEYS) == set(contract.SETTINGS)
    out = resolve_env.resolve({"COGNITO_ALLOWED_SUBS": "live", "MCP_API_KEY": "x"},
                              {"RECORDS_TABLE_NAME": "new", "FUNCTION_NAME": "not-a-function-setting"})
    assert out["COGNITO_ALLOWED_SUBS"] == "live" and out["RECORDS_TABLE_NAME"] == "new"
    assert "MCP_API_KEY" not in out and "FUNCTION_NAME" not in out


def test_allowed_subs_guard_is_preserved():
    env = full_env(COGNITO_ALLOWED_SUBS=" , ")
    msg = resolve_env.guard(env)
    assert "COGNITO_ALLOWED_SUBS is empty" in msg


def test_a_setting_absent_from_live_and_supplied_is_caught_by_resolve_then_guard():
    live = {k: "v" for k in contract.SETTINGS if k != REQUIRED[0]}
    assert REQUIRED[0] in resolve_env.guard(resolve_env.resolve(live, {}))
    assert resolve_env.guard(resolve_env.resolve(live, {REQUIRED[0]: "supplied"})) == ""


def _fake_aws(env_vars):
    def aws(*args, check=True):
        if "get-function-configuration" in args:
            return json.dumps({"CodeSha256": "abc=", "LastModified": "t", "Environment": {"Variables": env_vars}})
        return json.dumps({"Versions": [{"Version": "$LATEST"}]})
    return aws


def _deploy_env(monkeypatch, drop=None):
    for k in list(os.environ):
        if k in contract.SETTINGS or k in resolve_env.DEPLOY_SETTINGS:
            monkeypatch.delenv(k, raising=False)
    for k in resolve_env.DEPLOY_SETTINGS:
        monkeypatch.setenv(k, "v-" + k.lower())
    for k in REQUIRED:
        monkeypatch.setenv(k, "v-" + k.lower())
    if drop:
        monkeypatch.delenv(drop, raising=False)


def test_plan_exits_zero_with_everything_supplied(monkeypatch, capsys):
    _deploy_env(monkeypatch)
    monkeypatch.setattr(release_ops, "aws", _fake_aws({}))
    assert release_ops.cmd_plan(None) == 0


@pytest.mark.parametrize("key", REQUIRED)
def test_plan_stops_the_deploy_when_one_required_setting_is_missing(monkeypatch, capsys, key):
    _deploy_env(monkeypatch, drop=key)
    monkeypatch.setattr(release_ops, "aws", _fake_aws({}))
    assert release_ops.cmd_plan(None) == 3
    out = capsys.readouterr().out
    assert "PLAN REFUSED" in out and key in out and "v-" not in out.replace("v-" + key.lower(), "")


@pytest.mark.parametrize("key", resolve_env.DEPLOY_SETTINGS)
def test_plan_stops_the_deploy_when_one_deploy_setting_is_missing(monkeypatch, capsys, key):
    _deploy_env(monkeypatch, drop=key)
    monkeypatch.setattr(release_ops, "aws", lambda *a, **k: pytest.fail("plan reached AWS with a setting missing"))
    assert release_ops.cmd_plan(None) == 3
    assert key in capsys.readouterr().out


def test_plan_passes_a_setting_that_only_the_live_function_holds(monkeypatch):
    _deploy_env(monkeypatch, drop="COGNITO_ALLOWED_SUBS")
    monkeypatch.setattr(release_ops, "aws", _fake_aws({"COGNITO_ALLOWED_SUBS": "live"}))
    assert release_ops.cmd_plan(None) == 0


WF = yaml.safe_load((ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text())


def test_workflow_plan_precedes_apply_and_cannot_be_skipped_or_ignored():
    steps = WF["jobs"]["release"]["steps"]
    names = [s["name"] for s in steps]
    plan = steps[next(i for i, n in enumerate(names) if n.startswith("Plan"))]
    apply_i = next(i for i, n in enumerate(names) if n.startswith("Apply"))
    assert names.index(plan["name"]) < apply_i
    assert "release_ops.py plan" in plan["run"]
    assert not plan.get("continue-on-error") and "if" not in plan
    # the settings are loaded (and a missing file refused) before anything else runs
    assert steps[1]["name"].startswith("Load deployment settings")
    assert names.index(steps[1]["name"]) < names.index(plan["name"])
    assert "if" not in steps[1] and not steps[1].get("continue-on-error")


def test_loader_refuses_a_missing_or_empty_settings_file(tmp_path, capsys):
    assert loader.main([str(tmp_path / "nope.env")]) == 2
    empty = tmp_path / "e.env"
    empty.write_text("# nothing\n")
    assert loader.main([str(empty)]) == 2
    bad = tmp_path / "b.env"
    bad.write_text("KEY=\n")
    assert loader.main([str(bad)]) == 2


def test_committed_settings_file_supplies_what_the_workflow_does_not():
    supplied = set(loader.parse((ROOT / "deploy" / "settings.env").read_text()))
    # sign-in pool binding comes from repository secrets and the allow-lists are preserved from the live function
    external = {"COGNITO_USER_POOL_ID", "COGNITO_CLIENT_ID", "COGNITO_ALLOWED_SUBS"}
    need = (set(REQUIRED) - external) | set(resolve_env.DEPLOY_SETTINGS)
    assert not need - supplied, f"deploy/settings.env lacks {sorted(need - supplied)}"
