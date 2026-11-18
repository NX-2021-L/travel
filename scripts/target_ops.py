#!/usr/bin/env python3
"""Second-target deploy helper for travel's one deploy path (MOM-TSK-007-0F, lane N6F).

A target is a settings file (deploy/targets/<name>.env) naming a region, a stack, a deploy role, a surface and
the settings of one internal deployment. Its account id is NEVER in the repository: the workflow supplies it in
TARGET_ACCOUNT_ID (from a repository variable) and every role ARN here is built from it at run time.

  plan [--discard]   create a change set for the stack and print it (read-only; nothing is applied). --discard
                     deletes the change set afterwards (a preview).
  apply              publish the restore point (when a release exists), execute the plan's change set, ship the
                     package with lambda:UpdateFunctionCode, write the outputs the undo needs.
  verify             the checks: function state, the one health read, no Function URL, no public permission,
                     the deployed package. There is no address to probe.
  health             the health read alone.
  role               print her deploy role ARN as the step output role_arn.

Values of environment variables are never printed.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
TEMPLATE = ROOT / "infra" / "cloudformation" / "travel-deployment.yaml"

REQUIRED = ("AWS_REGION", "STACK_NAME", "DEPLOY_ROLE_NAME", "TABLE_NAME", "RECORDS_TABLE_NAME", "SURFACE_NAME",
            "FUNCTION_NAME", "LAMBDA_ROLE_NAME", "INTERNAL_INVOKER_ROLE_NAME", "ACCOUNT_ID_VARIABLE")
ACCOUNT_ID = re.compile(r"^[0-9]{12}$")
HEALTH_METHOD = "ping"


class TargetError(Exception):
    pass


def check_settings(env: dict) -> dict:
    """The target's settings, or TargetError. Internal door only: this path never makes a public address."""
    absent = [k for k in REQUIRED if not (env.get(k) or "").strip()]
    if absent:
        raise TargetError("target settings absent: " + ", ".join(sorted(absent)))
    if env.get("DOOR_MODE") != "internal":
        raise TargetError("target settings refused: DOOR_MODE must be internal (this path makes no public address)")
    acct = (env.get("TARGET_ACCOUNT_ID") or "").strip()
    if not ACCOUNT_ID.match(acct):
        raise TargetError("TARGET_ACCOUNT_ID is unset or not 12 digits: set the repository variable named by "
                          f"ACCOUNT_ID_VARIABLE ({env.get('ACCOUNT_ID_VARIABLE')})")
    return env


def role_arn(account_id: str, role_name: str) -> str:
    if "*" in role_name or "*" in account_id:
        raise TargetError("a role ARN with a wildcard is refused")
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def stack_parameters(env: dict) -> list:
    """The template's parameters for this target. No invoker is granted: InvokerGrant is false."""
    acct = env["TARGET_ACCOUNT_ID"]
    pairs = [
        ("FlightsTableName", env["TABLE_NAME"]), ("RecordsTableName", env["RECORDS_TABLE_NAME"]),
        ("FunctionName", env["FUNCTION_NAME"]), ("RoleName", env["LAMBDA_ROLE_NAME"]),
        ("SurfaceName", env["SURFACE_NAME"]), ("PublicEdge", "false"), ("InvokerGrant", "false"),
        ("InternalInvokerRole", role_arn(acct, env["INTERNAL_INVOKER_ROLE_NAME"])),
        ("InternalDeployRole", role_arn(acct, env["DEPLOY_ROLE_NAME"])),
    ]
    if env.get("FINANCE_READ_FUNCTION"):
        pairs.append(("FinanceReadFunction", env["FINANCE_READ_FUNCTION"]))
    return [{"ParameterKey": k, "ParameterValue": v} for k, v in pairs]


def health_event(deploy_role: str) -> dict:
    return {"invoker_role": deploy_role, "mcp": {"jsonrpc": "2.0", "id": 1, "method": HEALTH_METHOD}}


def health_problem(response, surface: str) -> str:
    """"" when the response is the health read of an internal door serving `surface`; else why not."""
    if not isinstance(response, dict) or response.get("statusCode"):
        return f"health read refused or malformed (statusCode {response.get('statusCode') if isinstance(response, dict) else '?'})"
    result = response.get("result") or {}
    if result.get("status") != "ok" or result.get("door") != "internal":
        return "health read did not answer ok through the internal door"
    if result.get("surface") != surface:
        return "health read names a different surface"
    return ""


def public_policy_problems(policy_json: str) -> list:
    """Statements of a resource policy that admit the public: a wildcard principal or a Function URL auth type."""
    out = []
    for st in (json.loads(policy_json).get("Statement") or []):
        p = st.get("Principal")
        wild = p == "*" or (isinstance(p, dict) and any(v == "*" or (isinstance(v, list) and "*" in v) for v in p.values()))
        cond = json.dumps(st.get("Condition") or {})
        if st.get("Effect") == "Allow" and (wild or "FunctionUrlAuthType" in cond or st.get("Action") == "lambda:InvokeFunctionUrl"):
            out.append(st.get("Sid") or "statement")
    return out


def render_changes(changes: list) -> str:
    lines = [f"  {c['ResourceChange'].get('Action', '?'):7s} {c['ResourceChange'].get('LogicalResourceId')} "
             f"({c['ResourceChange'].get('ResourceType')}) replacement={c['ResourceChange'].get('Replacement', 'n/a')}"
             for c in changes if c.get("Type") == "Resource"]
    return "\n".join(lines) or "  (no changes)"


# --------------------------------------------------------------------------- aws
# A hang must never be silent (run 44370894800: apply sat for an hour with no output). Every aws call is logged
# with a timestamp (service and operation only, never a value), has a pager-less CLI with connect and read
# timeouts, and is killed by a hard wall-clock timeout; waiting is our own bounded loop, never `aws ... wait`.
STUB_CODE_BYTES = 10_000  # the template's refusing stub is a few hundred bytes; a shipped package is megabytes


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%SZ", time.gmtime()) + " [target_ops] " + msg, flush=True)


def aws(*args, region, check=True, timeout=120, read_timeout=60):
    op = " ".join(a for a in args[:2] if not a.startswith("-"))
    log(f"aws {op} (limit {timeout}s)")
    cmd = ["aws", "--no-cli-pager", "--cli-connect-timeout", "15", "--cli-read-timeout", str(read_timeout), *args, "--region", region]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        log(f"TIMEOUT: aws {op} did not finish in {timeout}s; stopped")
        raise SystemExit(124)
    log(f"aws {op} -> rc {r.returncode}")
    if check and r.returncode != 0:
        print(r.stderr, file=sys.stderr, flush=True)
        raise SystemExit(r.returncode)
    return r


def is_stub(code_size) -> bool:
    """True when the function still holds the template's refusing stub: there is no release to restore."""
    try:
        return int(code_size) < STUB_CODE_BYTES
    except (TypeError, ValueError):
        return False


def stack_wait_state(status: str) -> str:
    """"done", "wait" or "fail" for a stack status while a change set executes."""
    if status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        return "done"
    if status.endswith("_IN_PROGRESS") and "ROLLBACK" not in status:
        return "wait"
    return "fail"


def wait_stack(env, deadline_s=900, interval_s=10) -> None:
    region, stack = env["AWS_REGION"], env["STACK_NAME"]
    end = time.time() + deadline_s
    while time.time() < end:
        st = aws("cloudformation", "describe-stacks", "--stack-name", stack, "--query", "Stacks[0].StackStatus", "--output", "text",
                 region=region, timeout=90).stdout.strip()
        log(f"stack {stack} is {st}")
        state = stack_wait_state(st)
        if state == "done":
            return
        if state == "fail":
            raise SystemExit(f"stack {stack} reached {st}")
        time.sleep(interval_s)
    raise SystemExit(f"stack {stack} did not finish within {deadline_s}s")


def wait_function(env, deadline_s=300, interval_s=5) -> None:
    region, fn = env["AWS_REGION"], env["FUNCTION_NAME"]
    end = time.time() + deadline_s
    while time.time() < end:
        out = aws("lambda", "get-function-configuration", "--function-name", fn, "--query", "[State,LastUpdateStatus]", "--output", "text",
                  region=region, timeout=90).stdout.split()
        log(f"function {fn} is {' '.join(out)}")
        if out == ["Active", "Successful"]:
            return
        if "Failed" in out:
            raise SystemExit(f"function {fn} update failed: {' '.join(out)}")
        time.sleep(interval_s)
    raise SystemExit(f"function {fn} did not become Active/Successful within {deadline_s}s")


def _env() -> dict:
    return check_settings(dict(os.environ))


def _change_set_name() -> str:
    return "travel-" + (os.environ.get("GITHUB_SHA", "local")[:7]) + "-" + os.environ.get("GITHUB_RUN_ID", "preview")


def stack_exists(env) -> bool:
    return aws("cloudformation", "describe-stacks", "--stack-name", env["STACK_NAME"], region=env["AWS_REGION"], check=False).returncode == 0


def cmd_plan(a) -> int:
    env = _env()
    region, stack = env["AWS_REGION"], env["STACK_NAME"]
    kind = "UPDATE" if stack_exists(env) else "CREATE"
    name = _change_set_name()
    aws("cloudformation", "create-change-set", "--stack-name", stack, "--change-set-name", name,
        "--change-set-type", kind, "--template-body", f"file://{TEMPLATE}",
        "--parameters", json.dumps(stack_parameters(env)), "--capabilities", "CAPABILITY_NAMED_IAM",
        "--output", "text", "--query", "Id", region=region)
    cs = {}
    for _ in range(30):  # bounded: at most about 5 minutes
        cs = json.loads(aws("cloudformation", "describe-change-set", "--stack-name", stack, "--change-set-name", name, region=region, timeout=90).stdout)
        if cs.get("Status") not in ("CREATE_PENDING", "CREATE_IN_PROGRESS"):
            break
        time.sleep(10)
    print(f"PLAN target: {env['TARGET_NAME']} stack {stack} ({region}), change set type {kind}, status {cs['Status']}")
    print(render_changes(cs.get("Changes") or []))
    if cs["Status"] == "FAILED":
        print("  reason: " + str(cs.get("StatusReason")))
    if a.discard or cs["Status"] == "FAILED":
        aws("cloudformation", "delete-change-set", "--stack-name", stack, "--change-set-name", name, region=region, check=False)
        if kind == "CREATE":  # a create change set leaves the stack in REVIEW_IN_PROGRESS; remove that empty shell
            aws("cloudformation", "delete-stack", "--stack-name", stack, region=region, check=False)
    return 0 if cs["Status"] in ("CREATE_COMPLETE", "AVAILABLE") or "didn't contain changes" in str(cs.get("StatusReason")) else 3


def _output(key, value):
    target = os.environ.get("GITHUB_OUTPUT")
    if target:
        with open(target, "a") as f:
            f.write(f"{key}={value}\n")
    print(f"{key}={value}")


def cmd_apply(a) -> int:
    env = _env()
    region, stack, fn = env["AWS_REGION"], env["STACK_NAME"], env["FUNCTION_NAME"]
    pre = ""
    log("apply 1/5: is there a release to restore?")
    cur = aws("lambda", "get-function-configuration", "--function-name", fn, "--query", "CodeSize", "--output", "text", region=region, check=False)
    if cur.returncode == 0 and not is_stub(cur.stdout.strip()):
        log("apply 2/5: publish the restore point")
        pre = aws("lambda", "publish-version", "--function-name", fn, "--description",
                  f"restore point before {os.environ.get('GITHUB_SHA', 'local')[:7]} (run {os.environ.get('GITHUB_RUN_ID', 'local')})",
                  "--query", "Version", "--output", "text", region=region).stdout.strip()
    else:
        log("apply 2/5: no shipped release (no function, or it holds the stub): no restore point")
    _output("pre_deploy_version", pre)
    _output("first_release", "false" if pre else "true")
    name = _change_set_name()
    cs = json.loads(aws("cloudformation", "describe-change-set", "--stack-name", stack, "--change-set-name", name, region=region, check=False).stdout or "{}")
    if cs.get("Status") == "CREATE_COMPLETE":
        log("apply 3/5: execute the change set and wait for the stack")
        aws("cloudformation", "execute-change-set", "--stack-name", stack, "--change-set-name", name, region=region)
        wait_stack(env)
    else:
        log("apply 3/5: no stack changes to apply")
    zip_path = os.environ.get("PACKAGE_ZIP", "")
    if not zip_path or not os.path.exists(zip_path):
        raise SystemExit("PACKAGE_ZIP is not set or missing: build it with PACKAGE_ONLY=<path> deploy.sh")
    log(f"apply 4/5: ship the package ({os.path.getsize(zip_path)} bytes)")
    wait_function(env)  # the stack's own create or update must have settled before code is written
    aws("lambda", "update-function-code", "--function-name", fn, "--zip-file", f"fileb://{zip_path}", "--output", "text", "--query", "FunctionArn",
        region=region, timeout=600, read_timeout=300)
    log("apply 5/5: wait for the new code to be active")
    wait_function(env)
    return 0


def health(env) -> str:
    deploy_role = role_arn(env["TARGET_ACCOUNT_ID"], env["DEPLOY_ROLE_NAME"])
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "out.json")
        r = aws("lambda", "invoke", "--function-name", env["FUNCTION_NAME"], "--payload", json.dumps(health_event(deploy_role)),
                "--cli-binary-format", "raw-in-base64-out", out, region=env["AWS_REGION"], check=False, timeout=90)
        if r.returncode != 0:
            return "health read could not be made: " + r.stderr.strip()[:200]
        try:
            response = json.load(open(out))
        except ValueError:
            return "health read returned no JSON"
    return health_problem(response, env["SURFACE_NAME"])


def cmd_role(_a) -> int:
    """Print her deploy role ARN as a step output (the account id is masked in the log)."""
    env = _env()
    _output("role_arn", role_arn(env["TARGET_ACCOUNT_ID"], env["DEPLOY_ROLE_NAME"]))
    return 0


def cmd_health(_a) -> int:
    msg = health(_env())
    print("CHECK FAILED: " + msg if msg else "health read: ok")
    return 1 if msg else 0


def cmd_verify(_a) -> int:
    env = _env()
    region, fn = env["AWS_REGION"], env["FUNCTION_NAME"]
    problems = []
    st = aws("lambda", "get-function-configuration", "--function-name", fn, "--query", "[State,LastUpdateStatus]", "--output", "text", region=region, check=False).stdout.split()
    if st != ["Active", "Successful"]:
        problems.append(f"function state ({' '.join(st) or 'unreadable'})")
    h = health(env)
    if h:
        problems.append(h)
    u = aws("lambda", "get-function-url-config", "--function-name", fn, region=region, check=False)
    if u.returncode == 0 or "ResourceNotFoundException" not in u.stderr:
        problems.append("a Function URL exists or its absence could not be read (an internal deployment has none)")
    p = aws("lambda", "get-policy", "--function-name", fn, "--query", "Policy", "--output", "text", region=region, check=False)
    if p.returncode == 0:
        bad = public_policy_problems(p.stdout)
        if bad:
            problems.append("public permission in the resource policy: " + ", ".join(bad))
    elif "ResourceNotFoundException" not in p.stderr:
        problems.append("the resource policy could not be read, so 'no public permission' is unproven")
    if subprocess.run([sys.executable, str(HERE / "verify_deployed_package.py")]).returncode != 0:
        problems.append("deployed package")
    if os.environ.get("FORCE_VERIFY_FAIL") == "true":
        problems.append("forced (FORCE_VERIFY_FAIL drill)")
    for m in problems:
        print("CHECK FAILED: " + m)
    return 1 if problems else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan"); p.add_argument("--discard", action="store_true"); p.set_defaults(fn=cmd_plan)
    sub.add_parser("apply").set_defaults(fn=cmd_apply)
    sub.add_parser("verify").set_defaults(fn=cmd_verify)
    sub.add_parser("health").set_defaults(fn=cmd_health)
    sub.add_parser("role").set_defaults(fn=cmd_role)
    a = ap.parse_args(argv)
    try:
        return a.fn(a)
    except TargetError as exc:
        print(f"TARGET REFUSED: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
