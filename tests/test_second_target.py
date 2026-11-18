"""MOM-TSK-007-0F (lane N6F): travel's deploy path gains a second target, and what finding F-10 left open is closed.
Offline: parses the workflow, the template, the target settings and the helper; no AWS call."""
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
import load_deployment_settings as lds  # noqa: E402
import resolve_env  # noqa: E402
import target_ops as to  # noqa: E402
from test_travel_deployment_template import R, T  # noqa: E402

WF = yaml.safe_load((ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text())
WF_TEXT = (ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text()
TARGET = lds.parse((ROOT / "deploy/targets/nana.env").read_text())
ACCT = "123456789012"
ENV = {**TARGET, "TARGET_ACCOUNT_ID": ACCT}


# ---- the target: a settings file, with no account id in it ----
def test_target_names_region_stack_role_surface_and_the_variable_that_holds_her_account():
    for k in ("AWS_REGION", "STACK_NAME", "DEPLOY_ROLE_NAME", "SURFACE_NAME", "ACCOUNT_ID_VARIABLE", "DOOR_MODE"):
        assert TARGET[k]
    assert TARGET["DOOR_MODE"] == "internal"
    assert f"vars.{TARGET['ACCOUNT_ID_VARIABLE']}" in WF_TEXT  # the workflow reads exactly the named variable


def test_her_account_id_is_in_no_file_of_the_repository():
    # The only twelve-digit value a target file may hold is io's CodeArtifact owner (shared code, ND-49).
    for path in (ROOT / "deploy" / "targets").glob("*.env"):
        for k, v in lds.parse(path.read_text()).items():
            assert k == "CODEARTIFACT_OWNER" or not re.search(r"\d{12}", v), f"{path.name}:{k}"
    assert not re.search(r"arn:aws:iam::\d{12}", WF_TEXT)
    assert not re.search(r"arn:aws:iam::\d{12}", (ROOT / "scripts/target_ops.py").read_text())


def test_internal_target_needs_no_probe_address():
    assert "PROBE_MCP_URL" not in TARGET and "PROBE_SIGNIN_URL" not in TARGET
    assert resolve_env.missing_deploy_settings(TARGET) == []
    public = {k: v for k, v in TARGET.items() if k != "DOOR_MODE"}  # a missing door reads as public: the strictest
    assert set(resolve_env.missing_deploy_settings(public)) == {"PROBE_MCP_URL", "PROBE_SIGNIN_URL"}
    io = lds.parse((ROOT / "deploy/settings.env").read_text())
    assert resolve_env.missing_deploy_settings(io) == []  # io's deployment is unchanged: still public, still probed
    assert set(resolve_env.missing_deploy_settings({**io, "PROBE_MCP_URL": ""})) == {"PROBE_MCP_URL"}


def test_the_helper_refuses_a_target_without_an_account_or_with_a_public_door():
    with pytest.raises(to.TargetError):
        to.check_settings(TARGET)  # no TARGET_ACCOUNT_ID
    with pytest.raises(to.TargetError):
        to.check_settings({**ENV, "TARGET_ACCOUNT_ID": "12345"})
    with pytest.raises(to.TargetError):
        to.check_settings({**ENV, "DOOR_MODE": "public"})
    with pytest.raises(to.TargetError):
        to.role_arn(ACCT, "role-*")
    assert to.check_settings(ENV)


def test_stack_parameters_name_no_invoker_and_each_role_exactly():
    p = {x["ParameterKey"]: x["ParameterValue"] for x in to.stack_parameters(ENV)}
    assert p["PublicEdge"] == "false" and p["InvokerGrant"] == "false"
    assert p["InternalDeployRole"] == f"arn:aws:iam::{ACCT}:role/{TARGET['DEPLOY_ROLE_NAME']}"
    assert p["SurfaceName"] == TARGET["SURFACE_NAME"]
    assert not any("*" in v for v in p.values())
    assert set(p) <= set(T["Parameters"])  # every parameter passed exists in the template


# ---- the workflow: one run, io's first, hers after, each under its own role ----
def test_hers_runs_after_io_and_only_when_io_passed():
    assert WF["jobs"]["nana"]["needs"] == "release"  # a failed release (io's check) leaves hers untouched
    assert "always()" not in str(WF["jobs"]["nana"].get("if", ""))


def test_each_job_assumes_its_own_role_afresh():
    def roles(job):
        return [s["with"]["role-to-assume"] for s in WF["jobs"][job]["steps"] if "with" in s and "role-to-assume" in s["with"]]
    assert roles("release") == ["${{ secrets.AWS_BACKEND_ROLE_TO_ASSUME }}"]
    assert roles("nana") == ["${{ steps.role.outputs.role_arn }}"]
    assert roles("undo-io") == ["${{ secrets.AWS_BACKEND_ROLE_TO_ASSUME }}"]  # io's role again, after hers failed
    assert roles("rollback-nana") == ["${{ steps.role.outputs.role_arn }}"]


def test_a_failed_check_on_hers_returns_both():
    nana = [s["name"] for s in WF["jobs"]["nana"]["steps"]]
    idx = {k: next(i for i, n in enumerate(nana) if n.startswith(k)) for k in ("Plan", "Apply", "Verify", "Undo")}
    assert idx["Plan"] < idx["Apply"] < idx["Verify"] < idx["Undo"]
    undo_io = WF["jobs"]["undo-io"]
    assert undo_io["needs"] == ["release", "nana"]
    assert "needs.nana.result == 'failure'" in undo_io["if"]
    assert WF["jobs"]["release"]["outputs"]["pre_deploy_version"]  # the restore point io's undo needs


def test_her_plan_is_shown_before_apply_and_changes_nothing():
    plan = next(s for s in WF["jobs"]["nana"]["steps"] if s["name"].startswith("Plan"))
    assert plan["run"].strip() == "python3 scripts/target_ops.py plan"
    src = (ROOT / "scripts/target_ops.py").read_text()
    assert "execute-change-set" not in src[src.index("def cmd_plan"):src.index("def _output")]


def test_her_check_has_no_probe_address():
    nana_text = yaml.safe_dump(WF["jobs"]["nana"])
    assert "PROBE_" not in nana_text and "mcp_post_deploy_probe" not in nana_text


def test_paths_and_concurrency_are_unchanged():
    assert WF[True]["push"]["paths"] == ["backend/lambda/travel_mcp/**", ".github/workflows/lambda-travel-mcp-deploy.yml"]
    assert WF["concurrency"] == {"group": "travel-mcp-deploy", "cancel-in-progress": False}


# ---- the health read and the public checks ----
def test_health_read_answers_only_ok_through_the_internal_door_for_the_named_surface():
    ev = to.health_event("arn:x")
    assert ev == {"invoker_role": "arn:x", "mcp": {"jsonrpc": "2.0", "id": 1, "method": "ping"}}
    ok = {"jsonrpc": "2.0", "id": 1, "result": {"status": "ok", "door": "internal", "surface": "s"}}
    assert to.health_problem(ok, "s") == ""
    assert to.health_problem(ok, "other")
    assert to.health_problem({"statusCode": 403}, "s")
    assert to.health_problem({"result": {"status": "ok", "door": "public", "surface": "s"}}, "s")


def test_public_permission_in_a_resource_policy_is_found():
    star = json.dumps({"Statement": [{"Sid": "a", "Effect": "Allow", "Principal": "*", "Action": "lambda:InvokeFunctionUrl"}]})
    aws_star = json.dumps({"Statement": [{"Sid": "b", "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "lambda:InvokeFunction"}]})
    one = json.dumps({"Statement": [{"Sid": "c", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::1:role/r"}, "Action": "lambda:InvokeFunction"}]})
    assert to.public_policy_problems(star) == ["a"] and to.public_policy_problems(aws_star) == ["b"]
    assert to.public_policy_problems(one) == []


def test_render_changes_lists_resources():
    out = to.render_changes([{"Type": "Resource", "ResourceChange": {"Action": "Add", "LogicalResourceId": "X", "ResourceType": "AWS::T"}}])
    assert "Add" in out and "X" in out


# ---- the template ----
@pytest.mark.parametrize("param", ["InternalInvokerRole", "InternalDeployRole"])
def test_role_parameters_refuse_a_wildcard(param):
    pat = re.compile(T["Parameters"][param]["AllowedPattern"])
    assert pat.match("") and pat.match("arn:aws:iam::123456789012:role/some-role")
    for bad in ("*", "arn:aws:iam::123456789012:role/*", "arn:aws:iam::*:role/r", "arn:aws:iam::123456789012:root", "role"):
        assert not pat.match(bad), bad


def test_surface_name_is_required_and_reaches_the_function():
    assert "Default" not in T["Parameters"]["SurfaceName"]
    assert R["McpFunction"]["Properties"]["Environment"]["Variables"]["SURFACE_NAME"] == {"Ref": "SurfaceName"}


def test_no_invoker_is_granted_unless_asked():
    assert T["Parameters"]["InvokerGrant"]["Default"] == "false"
    cond = T["Conditions"]["GrantsInvoker"]["Fn::And"]
    assert {"Fn::Equals": [{"Ref": "InvokerGrant"}, "true"]} in cond


def test_the_function_can_be_made_without_a_code_bucket():
    code = R["McpFunction"]["Properties"]["Code"]
    assert T["Parameters"]["CodeS3Bucket"]["Default"] == ""
    assert "ZipFile" in code and code["S3Bucket"]["Fn::If"][0] == "HasCodeBucket"


# ---- the deploy script ----
def test_deploy_script_makes_a_function_url_only_for_a_public_door():
    sh = (ROOT / "backend/lambda/travel_mcp/deploy.sh").read_text()
    guard = sh.index('"${DOOR_MODE:-public}" = "internal"')
    assert guard < sh.index("create-function-url-config") and guard < sh.index("add-permission")
    assert 'PACKAGE_ONLY' in sh and sh.index("PACKAGE_ONLY") < sh.index("get-function-configuration")


# ---- a hang is never silent and never unbounded (run 44370894800: apply sat for an hour printing nothing) ----
def test_every_aws_call_is_pagerless_bounded_and_logged(monkeypatch, capsys):
    seen = {}

    def fake_run(cmd, **kw):
        seen["cmd"], seen["kw"] = cmd, kw
        class R:  # noqa: E306
            returncode, stdout, stderr = 0, "", ""
        return R()
    monkeypatch.setattr(to.subprocess, "run", fake_run)
    to.aws("lambda", "get-function", "--function-name", "f", region="r", timeout=7)
    assert "--no-cli-pager" in seen["cmd"] and "--cli-read-timeout" in seen["cmd"] and "--cli-connect-timeout" in seen["cmd"]
    assert seen["kw"]["timeout"] == 7 and seen["kw"]["stdin"] == to.subprocess.DEVNULL
    out = capsys.readouterr().out
    assert "aws lambda get-function" in out and "--function-name" not in out and "f" not in out.split("get-function")[1].split("(")[0]


def test_a_timed_out_aws_call_stops_the_run_loudly(monkeypatch, capsys):
    def boom(cmd, **kw):
        raise to.subprocess.TimeoutExpired(cmd, kw["timeout"])
    monkeypatch.setattr(to.subprocess, "run", boom)
    with pytest.raises(SystemExit) as e:
        to.aws("cloudformation", "describe-stacks", region="r", timeout=1)
    assert e.value.code == 124 and "TIMEOUT" in capsys.readouterr().out


def test_there_is_no_aws_waiter_left_in_her_path():
    src = (ROOT / "scripts/target_ops.py").read_text()
    assert '"cloudformation", "wait"' not in src and '"lambda", "wait"' not in src


def test_stack_wait_states():
    assert to.stack_wait_state("CREATE_COMPLETE") == to.stack_wait_state("UPDATE_COMPLETE") == "done"
    assert to.stack_wait_state("CREATE_IN_PROGRESS") == "wait"
    for bad in ("ROLLBACK_COMPLETE", "ROLLBACK_IN_PROGRESS", "CREATE_FAILED", "UPDATE_ROLLBACK_COMPLETE"):
        assert to.stack_wait_state(bad) == "fail"


def test_the_stub_is_not_a_restore_point():
    assert to.is_stub(216) and to.is_stub("216")
    assert not to.is_stub(41_000_000) and not to.is_stub("None")


def test_her_jobs_are_bounded_and_unbuffered_and_io_job_is_untouched():
    nana = WF["jobs"]["nana"]
    assert nana["timeout-minutes"] and nana["env"]["PYTHONUNBUFFERED"] == "1" and nana["env"]["AWS_PAGER"] == ""
    assert all("timeout-minutes" in s for s in nana["steps"] if s["name"].split(" ")[0] in ("Build", "Plan", "Apply", "Verify", "Undo"))
    assert "timeout-minutes" not in WF["jobs"]["release"]


# ---- run 44382258007: the package check must start the internal door ----
def test_package_check_carries_the_invoker_and_deploy_role_arns_built_at_run_time():
    e = to.package_check_env(ENV, {"X": "1"})
    assert e["X"] == "1"
    assert e["INTERNAL_INVOKER_ROLE"] == f"arn:aws:iam::{ACCT}:role/{TARGET['INTERNAL_INVOKER_ROLE_NAME']}"
    assert e["INTERNAL_DEPLOY_ROLE"] == f"arn:aws:iam::{ACCT}:role/{TARGET['DEPLOY_ROLE_NAME']}"
    p = {x["ParameterKey"]: x["ParameterValue"] for x in to.stack_parameters(ENV)}  # the same ARNs the function is given
    assert (e["INTERNAL_INVOKER_ROLE"], e["INTERNAL_DEPLOY_ROLE"]) == (p["InternalInvokerRole"], p["InternalDeployRole"])
    assert p["InvokerGrant"] == "false"  # named, granted nothing


def test_verify_runs_the_package_check_under_that_environment():
    src = (ROOT / "scripts/target_ops.py").read_text()
    assert "verify_deployed_package.py" in src and "env=package_check_env(env, dict(os.environ))" in src


def test_the_door_starts_with_the_package_check_environment():
    import door
    e = to.package_check_env(ENV, {"DOOR_MODE": "internal"})
    assert door.load(e)[0] == "internal"
    with pytest.raises(door.DoorError):
        door.load({"DOOR_MODE": "internal"})  # what run 44382258007 had


def test_a_function_error_is_not_a_health_answer(monkeypatch):
    class R:
        returncode, stdout, stderr = 0, "Unhandled\n", ""
    monkeypatch.setattr(to, "aws", lambda *a, **k: R())
    assert "function failed" in to.health(ENV)
