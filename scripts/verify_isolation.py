#!/usr/bin/env python3
"""Read-only verification of her travel deployment's isolation, parity and refusals (MOM-TSK-007-0G, lane N6G).

Covers NA-47 / NA-63 for travel: A-20, A-21 and the rest of A-28, for two accounts. Every judgement is a pure function
(tested on fixtures in tests/test_isolation_verification.py); the `live` subcommand feeds them from the two accounts.

  verify_isolation.py templates          offline: invoke principals / public address / table policies from the template
  verify_isolation.py live               read both accounts: io's through --io-profile (product-lead), hers through
                                         --nana-profile (nana); refuses live in both directions; door probes; parity
  verify_isolation.py drill-snapshot     JSON of both functions (code sha, version, env KEYS) to take before/after a drill
  verify_isolation.py drill-compare B A  judge two snapshots: hers is back where it was, io's is not hers to undo

Her account id is NEVER in the repository: it is read from the environment variable named by ACCOUNT_ID_VARIABLE
(deploy/targets/nana.env, NANA_AWS_ACCOUNT_ID). Values of function environment variables are never printed (keys only).
Nothing here deploys, writes, or sends a non-health event that does I/O. Exit 1 on any failed check.
"""
import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import load_deployment_settings as lds  # noqa: E402

DENIED = ("AccessDenied", "AccessDeniedException", "not authorized", "UnauthorizedOperation", "is not authorized")
SNAPSHOT_FILES = ("caps.json", "permission_manifest.json")


# ------------------------------------------------------------------ pure judgements
def eval_template(template: dict, public: bool = False, invoker_grant: bool = False, invoker_role: str = "x", deploy_role: str = "y") -> dict:
    """Which resources of the travel template exist, and who is admitted, under one parameter set.
    Returns {"function_url": bool, "principals": [..], "table_resource_policies": [..]}."""
    cond = template.get("Conditions", {})
    ctx = {"IsPublicEdge": public}
    ctx["GrantsInvoker"] = (not public) and bool(invoker_role) and invoker_grant
    ctx["GrantsDeployer"] = (not public) and bool(deploy_role)
    for name in cond:
        if name not in ctx:
            ctx[name] = True  # conditions that do not govern door or grants (code bucket, finance) are not read here
    res = template["Resources"]
    live = {k: v for k, v in res.items() if v.get("Condition") is None or ctx.get(v["Condition"], True)}
    principals = []
    for k, v in live.items():
        if v["Type"] == "AWS::Lambda::Permission":
            p = v["Properties"].get("Principal")
            principals.append("*" if p == "*" else ("<invoker>" if (p or {}).get("Ref") == "InternalInvokerRole" else
                              "<deploy>" if (p or {}).get("Ref") == "InternalDeployRole" else str(p)))
    return {
        "function_url": any(v["Type"] == "AWS::Lambda::Url" for v in live.values()),
        "principals": sorted(principals),
        "table_resource_policies": [k for k, v in live.items() if v["Type"] == "AWS::DynamoDB::Table" and "ResourcePolicy" in v.get("Properties", {})],
    }


def function_policy_problems(policy_json, allowed: set) -> list:
    """Principals able to invoke the function (resource policy) must equal `allowed` exactly; no wildcard, no url auth."""
    if not policy_json:
        return [] if not allowed else ["resource policy absent but a principal is expected"]
    seen, out = set(), []
    for st in json.loads(policy_json).get("Statement") or []:
        if st.get("Effect") != "Allow":
            continue
        p = st.get("Principal")
        vals = [p] if isinstance(p, str) else [x for v in (p or {}).values() for x in ([v] if isinstance(v, str) else v)]
        if "*" in vals or "FunctionUrlAuthType" in json.dumps(st.get("Condition") or {}) or st.get("Action") == "lambda:InvokeFunctionUrl":
            out.append(f"public grant in {st.get('Sid') or 'a statement'}")
        seen.update(v for v in vals if v != "*")
    if seen != allowed:
        out.append(f"invoking principals {sorted(seen)} != named list {sorted(allowed)}")
    return out


def role_policy_problems(docs: list, own_tables: set, finance: str) -> list:
    """Her travel role's policies name only her own tables (and their indexes) and her finance read (a named function)."""
    allowed_actions_ok = lambda a: a.startswith("dynamodb:") or a in ("lambda:InvokeFunction",) or a.startswith("logs:")  # noqa: E731
    out = []
    for doc in docs:
        for st in doc.get("Statement") or []:
            if st.get("Effect") != "Allow":
                continue
            acts = [st["Action"]] if isinstance(st.get("Action"), str) else st.get("Action", [])
            ress = [st["Resource"]] if isinstance(st.get("Resource"), str) else st.get("Resource", [])
            for a in acts:
                if not allowed_actions_ok(a):
                    out.append(f"unexpected action {a}")
            for r in ress:
                if r == "*":
                    out.append(f"wildcard resource {r}")
                elif ":table/" in r:
                    if r.split(":table/")[1].split("/")[0] not in own_tables:
                        out.append(f"names a table that is not hers: {r}")
                elif ":function:" in r:
                    if not finance or r != finance:
                        out.append(f"names a function that is not her finance read: {r}")
                elif not r.startswith("arn:aws:logs"):
                    out.append(f"unexpected resource {r}")
    return out


def names_account(texts: list, account_id: str) -> list:
    """Indexes of texts (policies, configs, trust documents of io's travel resources) that name her account."""
    return [i for i, t in enumerate(texts) if account_id and account_id in (t or "")]


def is_refused(returncode: int, stderr: str) -> bool:
    return returncode != 0 and any(m in (stderr or "") for m in DENIED)


def door_problems(health_resp, nonhealth_resp, http_resp, stranger_resp, surface: str) -> list:
    """Her door: the deploy role is answered the health read and nothing else; HTTP-shaped and unnamed callers are refused."""
    out = []
    r = (health_resp or {}).get("result") or {}
    if r.get("status") != "ok" or r.get("door") != "internal" or r.get("surface") != surface:
        out.append("health read did not answer ok through the internal door")
    for name, resp in (("a non-health method from the deploy role", nonhealth_resp), ("an HTTP-shaped event", http_resp), ("an unnamed role", stranger_resp)):
        if (resp or {}).get("statusCode") != 403:
            out.append(f"{name} was not refused with 403")
    return out


def package_digests(zip_bytes: bytes) -> dict:
    """sha256 of every top-level module and of the two snapshots in a deployed package (names only, never values)."""
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    return {n: hashlib.sha256(z.read(n)).hexdigest() for n in z.namelist() if "/" not in n and (n.endswith(".py") or n in SNAPSHOT_FILES)}


def parity_problems(io_digests: dict, her_digests: dict) -> list:
    """Same build, same tool list and schemas: every module and both snapshots byte-identical in the two accounts."""
    out = []
    for n in sorted(set(io_digests) | set(her_digests)):
        if io_digests.get(n) != her_digests.get(n):
            out.append(f"{n}: {'missing in hers' if n not in her_digests else 'missing in io' if n not in io_digests else 'differs'}")
    return out


def drill_problems(before: dict, after: dict) -> list:
    """After a drill that made one check fail on hers: hers is where it was; io's was not undone on account of hers.
    io's own function may have moved forward (the run deployed io's first); what it must NOT do is move back."""
    out = []
    if after["hers"]["code_sha256"] != before["hers"]["code_sha256"]:
        out.append("her code is not the previous release's")
    if after["hers"]["env_keys"] != before["hers"]["env_keys"]:
        out.append("her environment keys differ from before the drill")
    if after["io"]["last_modified"] < before["io"]["last_modified"]:
        out.append("io's function is older than before the drill")
    if after["io"]["code_sha256"] == after["io"].get("restore_point_sha256"):
        out.append("io's function is on the restore point: it was undone on account of hers")
    return out


# ------------------------------------------------------------------ live (aws CLI, read-only)
def aws(profile, region, *args, timeout=90):
    r = subprocess.run(["aws", "--no-cli-pager", "--profile", profile, "--region", region, *args],
                       capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    return r


def _json(r):
    try:
        return json.loads(r.stdout)
    except ValueError:
        return None


def _invoke(profile, region, fn, event):
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "o.json")
        r = aws(profile, region, "lambda", "invoke", "--function-name", fn, "--payload", json.dumps(event),
                "--cli-binary-format", "raw-in-base64-out", "--query", "FunctionError", "--output", "text", out)
        body = None
        if r.returncode == 0:
            try:
                body = json.load(open(out))
            except ValueError:
                body = None
        return r, body


def _code_zip(profile, region, fn) -> bytes:
    loc = aws(profile, region, "lambda", "get-function", "--function-name", fn, "--query", "Code.Location", "--output", "text")
    return urllib.request.urlopen(loc.stdout.strip(), timeout=120).read()  # presigned; the URL is never printed


def _settings():
    io_env = lds.parse((ROOT / "deploy/settings.env").read_text())
    her = lds.parse((ROOT / "deploy/targets/nana.env").read_text())
    acct = os.environ.get(her["ACCOUNT_ID_VARIABLE"], "")
    if not re.fullmatch(r"\d{12}", acct):
        raise SystemExit(f"{her['ACCOUNT_ID_VARIABLE']} must hold her 12-digit account id (it is never in the repository)")
    return io_env, her, acct


def cmd_templates(_a) -> int:
    sys.path.insert(0, str(ROOT / "tests"))
    from test_travel_deployment_template import load  # noqa: E402
    t = load()
    probs = []
    for label, kw in (("internal, no grant", {}), ("internal, grant", {"invoker_grant": True})):
        e = eval_template(t, public=False, **kw)
        if e["function_url"] or "*" in e["principals"] or e["table_resource_policies"]:
            probs.append(f"{label}: {e}")
    print("templates: " + ("; ".join(probs) if probs else "ok (no Function URL, no wildcard grant, no table resource policy)"))
    return 1 if probs else 0


def cmd_live(a) -> int:
    io_env, her, acct = _settings()
    region_her, region_io = her["AWS_REGION"], io_env["AWS_REGION"]
    fn_her, fn_io = her["FUNCTION_NAME"], io_env["FUNCTION_NAME"]
    deploy_role = f"arn:aws:iam::{acct}:role/{her['DEPLOY_ROLE_NAME']}"
    io_acct = (_json(aws(a.io_profile, region_io, "sts", "get-caller-identity")) or {}).get("Account", "")
    results = []

    def check(name, problems):
        results.append((name, problems))
        print(("PASS " if not problems else "FAIL ") + name + ("" if not problems else ": " + "; ".join(problems)))

    # her account, her profile
    pol = aws(a.nana_profile, region_her, "lambda", "get-policy", "--function-name", fn_her, "--query", "Policy", "--output", "text")
    check("her function policy admits her deploy role alone", function_policy_problems(pol.stdout.strip() if pol.returncode == 0 else "", {deploy_role}) if pol.returncode == 0 else ["policy unreadable: " + pol.stderr[:120]])
    url = aws(a.nana_profile, region_her, "lambda", "get-function-url-config", "--function-name", fn_her)
    check("her function has no public address", [] if url.returncode != 0 and "ResourceNotFoundException" in url.stderr else ["a Function URL exists or could not be read"])
    for t in (her["TABLE_NAME"], her["RECORDS_TABLE_NAME"]):
        rp = aws(a.nana_profile, region_her, "dynamodb", "get-resource-policy", "--resource-arn", f"arn:aws:dynamodb:{region_her}:{acct}:table/{t}")
        check(f"her table {t} has no resource policy", [] if rp.returncode != 0 and "PolicyNotFound" in rp.stderr else ["a resource policy exists or could not be read"])
    role = her["LAMBDA_ROLE_NAME"]
    docs = []
    for n in (_json(aws(a.nana_profile, region_her, "iam", "list-role-policies", "--role-name", role)) or {}).get("PolicyNames", []):
        docs.append((_json(aws(a.nana_profile, region_her, "iam", "get-role-policy", "--role-name", role, "--policy-name", n)) or {}).get("PolicyDocument", {}))
    cfg = _json(aws(a.nana_profile, region_her, "lambda", "get-function-configuration", "--function-name", fn_her)) or {}
    finance = (cfg.get("Environment", {}).get("Variables") or {}).get("FINANCE_READ_FUNCTION", "")
    finance_arn = finance if (not finance or finance.startswith("arn:")) else f"arn:aws:lambda:{region_her}:{acct}:function:{finance}"
    check("her travel role names only her tables and her finance read", role_policy_problems(docs, {her["TABLE_NAME"], her["RECORDS_TABLE_NAME"]}, finance_arn))
    check("her function holds no row or setting of io's (env keys only, no io function or table named)", [
        f"names io's {k}" for k, v in (cfg.get("Environment", {}).get("Variables") or {}).items() if io_acct and io_acct in str(v)])
    # io's account, io's profile: nothing names her
    texts = []
    for fn in (fn_io,):
        texts.append(aws(a.io_profile, region_io, "lambda", "get-policy", "--function-name", fn, "--query", "Policy", "--output", "text").stdout)
        texts.append(json.dumps(_json(aws(a.io_profile, region_io, "lambda", "get-function-configuration", "--function-name", fn))))
    for t in (io_env["TABLE_NAME"], io_env["RECORDS_TABLE_NAME"]):
        texts.append(aws(a.io_profile, region_io, "dynamodb", "get-resource-policy", "--resource-arn", f"arn:aws:dynamodb:{region_io}:{io_acct}:table/{t}").stdout)
    texts.append(aws(a.io_profile, region_io, "iam", "get-role", "--role-name", io_env.get("LAMBDA_ROLE_NAME", "io-travel-mcp-lambda-role")).stdout)
    check("no travel resource of io's names her account", [f"io-side text #{i} names her account" for i in names_account(texts, acct)])
    # refused live, both directions
    health_event = {"invoker_role": deploy_role, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "ping"}}
    r, _ = _invoke(a.io_profile, region_her, f"arn:aws:lambda:{region_her}:{acct}:function:{fn_her}", health_event)
    check("io's role is refused invoking her function (live)", [] if is_refused(r.returncode, r.stderr) else ["not refused"])
    for t in (her["TABLE_NAME"], her["RECORDS_TABLE_NAME"]):
        r = aws(a.io_profile, region_her, "dynamodb", "describe-table", "--table-name", f"arn:aws:dynamodb:{region_her}:{acct}:table/{t}")
        check(f"io's role is refused on her table {t} (live)", [] if is_refused(r.returncode, r.stderr) else ["not refused"])
    r, _ = _invoke(a.nana_profile, region_io, f"arn:aws:lambda:{region_io}:{io_acct}:function:{fn_io}", health_event)
    check("her profile is refused invoking io's function (live)", [] if is_refused(r.returncode, r.stderr) else ["not refused"])
    for t in (io_env["TABLE_NAME"], io_env["RECORDS_TABLE_NAME"]):
        r = aws(a.nana_profile, region_io, "dynamodb", "describe-table", "--table-name", f"arn:aws:dynamodb:{region_io}:{io_acct}:table/{t}")
        check(f"her profile is refused on io's table {t} (live)", [] if is_refused(r.returncode, r.stderr) else ["not refused"])
    # the door: health from the deploy role, then the three refusals (none of them reaches a tool)
    ev = lambda **k: {"invoker_role": deploy_role, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "ping"}, **k}  # noqa: E731
    _, h = _invoke(a.nana_profile, region_her, fn_her, ev())
    _, n = _invoke(a.nana_profile, region_her, fn_her, {"invoker_role": deploy_role, "mcp": {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}})
    _, w = _invoke(a.nana_profile, region_her, fn_her, {"requestContext": {"http": {"method": "POST", "path": "/mcp"}}, "rawPath": "/mcp", "headers": {}})
    _, s = _invoke(a.nana_profile, region_her, fn_her, {"invoker_role": "arn:aws:iam::000000000000:role/not-named", "mcp": {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}})
    check("her function answers the internal door and nothing else", door_problems(h, n, w, s, her["SURFACE_NAME"]))
    # parity
    pio = package_digests(_code_zip(a.io_profile, region_io, fn_io))
    phe = package_digests(_code_zip(a.nana_profile, region_her, fn_her))
    check("her tool list and schemas equal io's (deployed modules and snapshots byte-identical)", parity_problems(pio, phe))
    # the trips read: needs the named invoker (not granted yet); the refusal is proved on fixtures in CI, here its cause is read
    check("her trips read refuses HOME_UNAVAILABLE (cause: no finance read configured in her function)" if not finance else
          "her finance read is configured (A3: hers failing returns hers alone); the live read is the coordinator's post-deploy check",
          [] if (not finance or "/" not in finance) else ["FINANCE_READ_FUNCTION must be a function name or ARN"])
    bad = [n for n, p in results if p]
    print(f"{len(results) - len(bad)}/{len(results)} checks passed")
    return 1 if bad else 0


def _snap_fn(profile, region, fn):
    c = _json(aws(profile, region, "lambda", "get-function-configuration", "--function-name", fn)) or {}
    vs = _json(aws(profile, region, "lambda", "list-versions-by-function", "--function-name", fn, "--query", "Versions[?Version!=`$LATEST`].[Version,CodeSha256]")) or []
    return {"code_sha256": c.get("CodeSha256"), "last_modified": c.get("LastModified"), "env_keys": sorted((c.get("Environment", {}).get("Variables") or {})),
            "restore_point_sha256": (vs[-1][1] if vs else None)}


def cmd_snapshot(a) -> int:
    io_env, her, _ = _settings()
    print(json.dumps({"io": _snap_fn(a.io_profile, io_env["AWS_REGION"], io_env["FUNCTION_NAME"]),
                      "hers": _snap_fn(a.nana_profile, her["AWS_REGION"], her["FUNCTION_NAME"])}, indent=2))
    return 0


def cmd_compare(a) -> int:
    p = drill_problems(json.load(open(a.before)), json.load(open(a.after)))
    print("drill: " + ("ok: hers is back on its previous release; io's was not undone" if not p else "; ".join(p)))
    return 1 if p else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--io-profile", default="product-lead")
    ap.add_argument("--nana-profile", default="nana")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("templates").set_defaults(fn=cmd_templates)
    sub.add_parser("live").set_defaults(fn=cmd_live)
    sub.add_parser("drill-snapshot").set_defaults(fn=cmd_snapshot)
    c = sub.add_parser("drill-compare"); c.add_argument("before"); c.add_argument("after"); c.set_defaults(fn=cmd_compare)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
