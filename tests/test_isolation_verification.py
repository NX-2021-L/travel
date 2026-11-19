"""MOM-TSK-007-0G (lane N6G): isolation, parity and refusal tests for her travel deployment, on fixtures. Offline: no AWS call.
The live reading is scripts/verify_isolation.py (read-only); here every judgement it makes is proved on both a clean
fixture and a broken one, so a check that cannot fail cannot pass either. The drill is NOT run here (it deploys)."""
import pathlib
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
import door  # noqa: E402
import load_deployment_settings as lds  # noqa: E402
import verify_isolation as vi  # noqa: E402
from test_travel_deployment_template import load  # noqa: E402

T = load()
HER = lds.parse((ROOT / "deploy/targets/nana.env").read_text())
IO = lds.parse((ROOT / "deploy/settings.env").read_text())
ACCT, OTHER = "123456789012", "210987654321"
DEPLOY = f"arn:aws:iam::{ACCT}:role/{HER['DEPLOY_ROLE_NAME']}"
INVOKER = f"arn:aws:iam::{ACCT}:role/{HER['INTERNAL_INVOKER_ROLE_NAME']}"
WF_TEXT = (ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text()
WF = yaml.safe_load(WF_TEXT)


def policy(*principals, action="lambda:InvokeFunction", cond=None):
    return json.dumps({"Statement": [{"Sid": f"s{i}", "Effect": "Allow", "Principal": {"AWS": p} if p != "*" else "*", "Action": action,
                                     **({"Condition": cond} if cond else {})} for i, p in enumerate(principals)]})


# ---- no public address, no public grant, principals equal a named list (NA-47 row 1) ----
def test_template_internal_has_no_url_no_wildcard_and_no_table_policy():
    for grant in (False, True):
        e = vi.eval_template(T, public=False, invoker_grant=grant)
        assert e["function_url"] is False and "*" not in e["principals"] and e["table_resource_policies"] == []


def test_template_principals_are_the_named_list_deploy_role_only_until_a_grant():
    assert vi.eval_template(T, public=False)["principals"] == ["<deploy>"]  # nobody else admitted before any caller is granted
    assert vi.eval_template(T, public=False, invoker_grant=True)["principals"] == ["<deploy>", "<invoker>"]
    assert vi.eval_template(T, public=True)["function_url"] is True  # the evaluator sees the public edge when it is on (so it can fail)
    assert "*" in vi.eval_template(T, public=True)["principals"]


def test_her_target_settings_grant_no_invoker():
    assert HER["DOOR_MODE"] == "internal" and HER["INTERNAL_INVOKER_ROLE_NAME"]
    assert "InvokerGrant" not in json.dumps(HER) and "PublicEdge" not in json.dumps(HER)


def test_function_policy_admits_her_deploy_role_alone():
    assert vi.function_policy_problems(policy(DEPLOY), {DEPLOY}) == []
    assert vi.function_policy_problems(policy(DEPLOY, INVOKER), {DEPLOY})  # one more principal than the list
    assert vi.function_policy_problems(policy(DEPLOY, f"arn:aws:iam::{OTHER}:role/io-x"), {DEPLOY})  # io's role admitted
    assert vi.function_policy_problems(policy("*"), {DEPLOY})  # public
    assert vi.function_policy_problems(policy(DEPLOY, action="lambda:InvokeFunctionUrl", cond={"StringEquals": {"lambda:FunctionUrlAuthType": "NONE"}}), {DEPLOY})
    assert vi.function_policy_problems("", {DEPLOY})  # empty where one is expected


# ---- her travel role names only her tables and her finance read ----
FIN = f"arn:aws:lambda:us-west-2:{ACCT}:function:her-finance-read"
TBL = lambda t: f"arn:aws:dynamodb:us-west-2:{ACCT}:table/{t}"  # noqa: E731
OWN = {HER["TABLE_NAME"], HER["RECORDS_TABLE_NAME"]}
GOOD_DOCS = [{"Statement": [{"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query"],
                              "Resource": [TBL(HER["TABLE_NAME"]), TBL(HER["TABLE_NAME"]) + "/index/*", TBL(HER["RECORDS_TABLE_NAME"])]},
                             {"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": FIN}]}]


def test_role_policy_clean_and_broken_fixtures():
    assert vi.role_policy_problems(GOOD_DOCS, OWN, FIN) == []
    bad_table = [{"Statement": [{"Effect": "Allow", "Action": "dynamodb:Scan", "Resource": TBL("io-travel-flights-x")}]}]
    assert vi.role_policy_problems(bad_table, OWN, FIN)
    star = [{"Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]}]
    assert vi.role_policy_problems(star, OWN, FIN)
    other_fn = [{"Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": f"arn:aws:lambda:us-west-2:{OTHER}:function:io-finance"}]}]
    assert vi.role_policy_problems(other_fn, OWN, FIN)
    assert vi.role_policy_problems([{"Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::x/*"}]}], OWN, FIN)


def test_template_role_policy_names_only_its_own_tables_and_the_finance_read():
    pol = [p for p in T["Resources"]["ExecutionRole"]["Properties"]["Policies"] if isinstance(p, dict) and "PolicyName" in p][0]
    ress = pol["PolicyDocument"]["Statement"][0]["Resource"]
    assert all(("FlightsTable" in json.dumps(r) or "RecordsTable" in json.dumps(r)) for r in ress)  # only the two tables of the stack
    fin = [p for p in T["Resources"]["ExecutionRole"]["Properties"]["Policies"] if isinstance(p, dict) and "Fn::If" in p][0]["Fn::If"]
    res = fin[1]["PolicyDocument"]["Statement"][0]["Resource"]
    assert res["Fn::If"][0] == "FinanceReadIsArn" and res["Fn::If"][1] == {"Ref": "FinanceReadFunction"}  # an ARN is used as given


# ---- nothing of io's names her account; refusals read live ----
def test_names_account_finds_her_account_in_io_side_text_and_only_there():
    assert vi.names_account(["{}", f"role arn:aws:iam::{ACCT}:root"], ACCT) == [1]
    assert vi.names_account(["{}", f"arn:aws:iam::{OTHER}:root"], ACCT) == []
    assert vi.names_account(["x"], "") == []


@pytest.mark.parametrize("rc,err,refused", [
    (254, "An error occurred (AccessDeniedException) when calling the Invoke operation: User is not authorized", True),
    (254, "An error occurred (AccessDeniedException) when calling the DescribeTable operation", True),
    (254, "An error occurred (ResourceNotFoundException) when calling the Invoke operation", False),  # not found is not a refusal
    (254, "Could not connect to the endpoint URL", False),  # an outage is not a refusal
    (0, "", False)])  # it worked: the isolation is broken
def test_a_refusal_is_an_access_denial_and_nothing_else(rc, err, refused):
    assert vi.is_refused(rc, err) is refused


# ---- the internal door and nothing else ----
OK = {"jsonrpc": "2.0", "id": 1, "result": {"status": "ok", "door": "internal", "surface": HER["SURFACE_NAME"]}}
R403 = {"statusCode": 403}


def test_door_problems_clean_and_broken():
    assert vi.door_problems(OK, R403, R403, R403, HER["SURFACE_NAME"]) == []
    assert vi.door_problems(OK, {"result": {}}, R403, R403, HER["SURFACE_NAME"])  # a tool answered the deploy role
    assert vi.door_problems(OK, R403, {"statusCode": 200}, R403, HER["SURFACE_NAME"])  # an HTTP-shaped event served
    assert vi.door_problems(OK, R403, R403, {"result": {}}, HER["SURFACE_NAME"])  # an unnamed role served
    assert vi.door_problems({**OK, "result": {**OK["result"], "door": "public"}}, R403, R403, R403, HER["SURFACE_NAME"])


def test_her_door_in_code_is_the_internal_door_and_nothing_else():
    http = {"requestContext": {"http": {"method": "POST", "path": "/mcp"}}, "rawPath": "/mcp", "headers": {}}
    health = {"invoker_role": DEPLOY, "mcp": {"method": door.HEALTH_METHOD, "id": 1}}
    assert door.admit(health, "internal", INVOKER, DEPLOY) == "health"
    with pytest.raises(door.InvokerRefused):  # the deploy role is answered the health read and nothing else
        door.admit({"invoker_role": DEPLOY, "mcp": {"method": "tools/list"}}, "internal", INVOKER, DEPLOY)
    with pytest.raises(door.HttpEventRefused):
        door.admit(http, "internal", INVOKER, DEPLOY)
    with pytest.raises(door.InvokerRefused):
        door.admit({"invoker_role": f"arn:aws:iam::{OTHER}:role/io-x", "mcp": {"method": "tools/list"}}, "internal", INVOKER, DEPLOY)
    with pytest.raises(door.DoorError):  # her settings with no named caller do not start
        door.load({"DOOR_MODE": "internal"})
    assert door.load({"DOOR_MODE": "internal", door.ROLE_KEY: INVOKER}) == ("internal", INVOKER)


# ---- parity: tool list and schemas equal io's ----
def _zip(files):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        for n, c in files.items():
            z.writestr(n, c)
    return b.getvalue()


BUILD = {"contract.py": "A=1", "lambda_function.py": "B=2", "caps.json": "{}", "permission_manifest.json": "{}", "requirements.txt": "x"}


def test_parity_between_two_deployed_packages_clean_and_broken():
    a = vi.package_digests(_zip(BUILD))
    assert "requirements.txt" not in a and set(a) == {"contract.py", "lambda_function.py", "caps.json", "permission_manifest.json"}
    assert vi.parity_problems(a, vi.package_digests(_zip(BUILD))) == []
    assert vi.parity_problems(a, vi.package_digests(_zip({**BUILD, "contract.py": "A=2"})))  # a schema differs
    assert vi.parity_problems(a, vi.package_digests(_zip({k: v for k, v in BUILD.items() if k != "caps.json"})))  # a snapshot missing


def _render(settings):
    import subprocess
    code = ("import asyncio,json,sys;sys.path.insert(0,sys.argv[1]);import lambda_function as lf;"
            "print(json.dumps([[t.model_dump(mode='json',exclude_none=True) for t in asyncio.run(lf.list_tools())],lf.build_caps(),lf.build_permission_manifest()],sort_keys=True))")
    keep = {k: os.environ[k] for k in ("PATH", "PYTHONPATH", "HOME") if k in os.environ}
    r = subprocess.run([sys.executable, "-c", code, str(ROOT / "backend/lambda/travel_mcp")], capture_output=True, text=True, env={**keep, **settings}, cwd=str(ROOT))
    assert r.returncode == 0, r.stderr[-1500:]
    return r.stdout


def test_her_real_settings_render_io_s_tool_list_and_schemas():
    pytest.importorskip("io_kit_mcp_edge")
    common = {"RECORDS_TABLE_NAME": "r", "TABLE_NAME": "t", "AWS_REGION": "us-west-2"}
    io_s = {**common, "SURFACE_NAME": IO["SURFACE_NAME"], "DOOR_MODE": "public", "COGNITO_USER_POOL_ID": "p", "COGNITO_CLIENT_ID": "c",
            "COGNITO_REGION": "us-west-2", "COGNITO_ALLOWED_SUBS": "s", "FINANCE_READ_FUNCTION": "f", "PUBLIC_BASE_URL": "https://x.invalid", "CORS_ORIGINS": "https://x.invalid"}
    her_s = {**common, "TABLE_NAME": HER["TABLE_NAME"], "RECORDS_TABLE_NAME": HER["RECORDS_TABLE_NAME"], "SURFACE_NAME": HER["SURFACE_NAME"],
             "DOOR_MODE": "internal", "INTERNAL_INVOKER_ROLE": INVOKER, "INTERNAL_DEPLOY_ROLE": DEPLOY}
    assert _render(io_s) == _render(her_s)


# ---- her deployment names her finance read by NAME (MOM-TSK-004-0H, lane N3H) ----
def test_her_target_names_her_finance_read_by_bare_name():
    assert HER["FINANCE_READ_FUNCTION"] == "finance-residence-read"  # a name: the function in her own account; no account id, no ARN
    import target_ops as to
    p = {x["ParameterKey"]: x["ParameterValue"] for x in to.stack_parameters({**HER, "TARGET_ACCOUNT_ID": ACCT})}
    assert p["FinanceReadFunction"] == "finance-residence-read"
    assert ACCT not in (ROOT / "deploy/targets/nana.env").read_text()


def test_template_grants_one_action_on_one_function_by_name_in_her_own_account():
    pols = [p for p in T["Resources"]["ExecutionRole"]["Properties"]["Policies"] if isinstance(p, dict) and "Fn::If" in p]
    assert len(pols) == 1
    st = pols[0]["Fn::If"][1]["PolicyDocument"]["Statement"]
    assert len(st) == 1 and st[0]["Action"] == "lambda:InvokeFunction" and st[0]["Effect"] == "Allow"
    arn = st[0]["Resource"]["Fn::If"][2]["Fn::Sub"]
    assert arn == "arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${FinanceReadFunction}"
    assert "*" not in arn and T["Conditions"]["FinanceReadIsArn"]  # one function, no wildcard


def test_role_policy_accepts_her_finance_read_built_from_the_bare_name():
    arn = f"arn:aws:lambda:us-west-2:{ACCT}:function:finance-residence-read"
    docs = [{"Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": arn}]}]
    assert vi.role_policy_problems(docs, OWN, arn) == []
    assert vi.role_policy_problems(docs, OWN, f"arn:aws:lambda:us-west-2:{ACCT}:function:finance-residence-read-writer")  # another function


def test_her_finance_read_failing_returns_hers_alone(monkeypatch):
    """A3: the failure of her read is HOME_UNAVAILABLE on her deployment; nothing of io's is touched."""
    import home_service as hs
    monkeypatch.setenv("FINANCE_READ_FUNCTION", "finance-residence-read")
    seen = []
    def boom(fn, payload, timeout):
        seen.append(fn)
        raise RuntimeError("down")
    with pytest.raises(hs.HomeError) as e:
        hs.read_home("2026-01-01", "2026-02-01", invoker=boom)
    assert e.value.code == "HOME_UNAVAILABLE" and seen == ["finance-residence-read"]  # one attempt, her function only


def test_trips_read_refuses_home_unavailable_without_a_finance_read(monkeypatch):
    pytest.importorskip("moto")
    import test_lane_3a_trips as t3
    import home_service as hs
    monkeypatch.delenv("FINANCE_READ_FUNCTION", raising=False)
    with pytest.raises(hs.HomeError) as e:
        hs.read_home("2026-01-01", "2026-02-01")  # her real configuration: nothing to ask
    assert e.value.code == "HOME_UNAVAILABLE"
    assert t3.contract.ERROR_CODES["HOME_UNAVAILABLE"] == 503


def test_list_trips_and_get_trip_refuse_home_unavailable_through_the_handlers(monkeypatch):
    pytest.importorskip("moto")
    import test_lane_3a_trips as t3
    gen = t3.world.__wrapped__(monkeypatch) if hasattr(t3.world, "__wrapped__") else None
    if gen is None:
        pytest.skip("pytest fixture wrapping differs")
    w = next(gen)
    try:
        t3.round_trip(w, "a", "2026-03-01", "2026-03-04")
        monkeypatch.delenv("FINANCE_READ_FUNCTION", raising=False)
        monkeypatch.setattr(t3.lane, "_read_home", lambda a, b: t3.hs.read_home(a, b))
        for tool, args in (("list_trips", {}), ("get_trip", {"trip_key": "a-out"})):
            out = t3.call(tool, args)
            assert t3.code(out) == "HOME_UNAVAILABLE" and out["error_envelope"]["retryable"] is True
    finally:
        next(gen, None)


# ---- the drill: prepared, not run ----
DRILL = json.loads((ROOT / "deploy/drills/n6g-nana-verify-fail.json").read_text())


def test_drill_input_uses_only_inputs_the_workflow_has_and_fails_hers_only():
    declared = (WF.get("on") or WF[True])["workflow_dispatch"]["inputs"]  # YAML 1.1 reads the key `on` as True
    for k, v in {**DRILL["inputs"], **DRILL["alternative_hers_alone"]}.items():
        assert k in declared, k
    assert DRILL["inputs"]["force_nana_verify_fail"] == "true" and "force_verify_fail" not in DRILL["inputs"]  # io's check is not failed
    assert DRILL["inputs"]["action"] in declared["action"]["options"]
    assert DRILL["alternative_hers_alone"]["action"] in declared["action"]["options"]
    assert DRILL["repo"] == "NX-2021-L/travel" and (ROOT / ".github/workflows" / DRILL["workflow"]).exists()
    assert not re.search(r"\d{12}", json.dumps(DRILL))  # her account id is in no file


def test_workflow_failure_of_hers_undoes_hers_and_never_io():
    nana = WF["jobs"]["nana"]
    steps = {s.get("id") or s.get("name"): s for s in nana["steps"]}
    undo = steps["undo"]
    assert undo["if"] == "steps.verify.outcome == 'failure'"
    assert "release_ops.py restore" in undo["run"]
    assert "FUNCTION_NAME" not in undo["run"] and "AWS_BACKEND_ROLE_TO_ASSUME" not in json.dumps(nana)  # her role only: it cannot touch io's
    rel = json.dumps(WF["jobs"]["release"])
    assert "force_nana_verify_fail" in json.dumps(nana["env"]) and "force_nana_verify_fail" not in rel


# ---- the drill judgement ----
def _snap(h, io_mod="2026-11-18T10:00:00", io_sha="NEW", rp="OLD", keys=("A", "B")):
    return {"hers": {"code_sha256": h, "env_keys": list(keys), "last_modified": "x"},
            "io": {"code_sha256": io_sha, "last_modified": io_mod, "restore_point_sha256": rp, "env_keys": []}}


def test_drill_compare_hers_back_and_io_not_undone():
    before = _snap("H1", io_mod="2026-11-18T09:00:00", io_sha="OLD", rp="OLD0")
    assert vi.drill_problems(before, _snap("H1")) == []
    assert vi.drill_problems(before, _snap("H2"))  # hers did not return
    assert vi.drill_problems(before, _snap("H1", keys=("A",)))
    assert vi.drill_problems(before, _snap("H1", io_sha="OLD", rp="OLD"))  # io's restored on account of hers


def test_verify_script_sends_the_doors_health_method():
    # The live check must send the door's own health method; a mismatch reads as a door failure.
    src = (pathlib.Path(__file__).resolve().parents[1] / "scripts" / "verify_isolation.py").read_text()
    assert f'"method": "{door.HEALTH_METHOD}"' in src
