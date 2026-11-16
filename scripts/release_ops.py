#!/usr/bin/env python3
"""Release plan / restore helper for the travel deploy path (FLY-TSK-027-0A).

The function has no alias (CloudFront fronts the unqualified Function URL, so $LATEST
serves live traffic and is outside this repo's control).  The way back is therefore a
version RESTORE onto $LATEST: fetch a published version's code and environment and
write them back to $LATEST.  Uses only permissions the deploy role holds.

Subcommands (all read-only except `restore`):
  plan                  print what a deploy would change; changes nothing
  highest-version       print the highest published version number ("" if none)
  restore [--version N] restore version N (default: highest published) onto $LATEST

Values of environment variables are never printed.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAMBDA_DIR = HERE.parent / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(LAMBDA_DIR))
import resolve_env  # noqa: E402

# FLY-TSK-028-0B: no deployment is named here; both come from the supplied settings. Empty when absent, and
# plan (below) then refuses before any AWS call.
FUNCTION = os.environ.get("FUNCTION_NAME", "")
REGION = os.environ.get("AWS_REGION", "")


def aws(*args, check=True):
    r = subprocess.run(["aws", *args, "--region", REGION], capture_output=True, text=True)
    if check and r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        raise SystemExit(r.returncode)
    return r.stdout


def highest_version(versions_json: dict) -> int:
    nums = [int(v["Version"]) for v in versions_json.get("Versions", []) if v.get("Version", "").isdigit()]
    return max(nums) if nums else 0


def render_plan(cfg: dict, highest: int, env_diff: list, requirements: list, region: str = REGION,
                function: str = FUNCTION) -> str:
    lines = [
        f"PLAN target: {function} ({region})",
        f"  current $LATEST CodeSha256: {cfg.get('CodeSha256')}",
        f"  current $LATEST LastModified: {cfg.get('LastModified')}",
        f"  highest published version: {highest if highest else 'none'}",
        f"  restore point to be published before apply: version {highest + 1 if highest else 1} (or the existing one if $LATEST is unchanged)",
        "  environment changes (keys only):",
    ]
    lines += [f"    {m:8s} {k}" for k, m in env_diff] or ["    (none)"]
    lines.append("  requirements:")
    lines += [f"    {r}" for r in requirements] or ["    (none)"]
    return "\n".join(lines)


def restore_config_args(version_cfg: dict, env_file: str) -> list:
    """Argument list for update-function-configuration that restores a version's settings."""
    args = ["lambda", "update-function-configuration", "--function-name", FUNCTION,
            "--environment", f"file://{env_file}"]
    for flag, key in (("--runtime", "Runtime"), ("--handler", "Handler"),
                      ("--memory-size", "MemorySize"), ("--timeout", "Timeout")):
        if version_cfg.get(key) is not None:
            args += [flag, str(version_cfg[key])]
    return args


def restore_code_args(zip_path: str) -> list:
    return ["lambda", "update-function-code", "--function-name", FUNCTION, "--zip-file", f"fileb://{zip_path}"]


def requirements_lines() -> list:
    p = LAMBDA_DIR / "requirements.txt"
    return [l.strip() for l in p.read_text().splitlines() if l.strip() and not l.startswith("#")]


def cmd_plan(_a) -> int:
    # Fail closed (A-2): a missing deploy setting stops the run here, before apply, naming KEYS only.
    absent = resolve_env.missing_deploy_settings()
    if absent:
        print("PLAN REFUSED: deploy settings absent: " + ", ".join(sorted(absent)))
        return 3
    cfg = json.loads(aws("lambda", "get-function-configuration", "--function-name", FUNCTION))
    versions = json.loads(aws("lambda", "list-versions-by-function", "--function-name", FUNCTION))
    current = (cfg.get("Environment") or {}).get("Variables") or {}
    resolved = resolve_env.resolve(current, os.environ)
    print(render_plan(cfg, highest_version(versions), resolve_env.diff_env(current, resolved), requirements_lines()))
    msg = resolve_env.guard(resolved)
    if msg:
        print(f"PLAN REFUSED: {msg}")
        return 3
    return 0


def cmd_highest(_a) -> int:
    versions = json.loads(aws("lambda", "list-versions-by-function", "--function-name", FUNCTION))
    h = highest_version(versions)
    print(h if h else "")
    return 0


def cmd_restore(a) -> int:
    version = a.version
    if not version:
        versions = json.loads(aws("lambda", "list-versions-by-function", "--function-name", FUNCTION))
        h = highest_version(versions)
        if not h:
            print("no published version to restore", file=sys.stderr)
            return 4
        version = str(h)
    print(f"RESTORE {FUNCTION} $LATEST <- version {version}")
    fn = json.loads(aws("lambda", "get-function", "--function-name", FUNCTION, "--qualifier", version))
    cfg = fn["Configuration"]
    with tempfile.TemporaryDirectory() as d:
        zip_path = os.path.join(d, "restore.zip")
        urllib.request.urlretrieve(fn["Code"]["Location"], zip_path)
        env_file = os.path.join(d, "env.json")
        fd = os.open(env_file, os.O_WRONLY | os.O_CREAT, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump({"Variables": (cfg.get("Environment") or {}).get("Variables") or {}}, f)
        aws(*restore_code_args(zip_path), "--output", "text", "--query", "FunctionArn")
        aws("lambda", "wait", "function-updated-v2", "--function-name", FUNCTION)
        aws(*restore_config_args(cfg, env_file), "--output", "text", "--query", "FunctionArn")
        aws("lambda", "wait", "function-updated-v2", "--function-name", FUNCTION)
    now = json.loads(aws("lambda", "get-function-configuration", "--function-name", FUNCTION))
    print(f"restored: version {version} CodeSha256 {cfg.get('CodeSha256')}; $LATEST CodeSha256 now {now.get('CodeSha256')}")
    if now.get("CodeSha256") != cfg.get("CodeSha256"):
        print("restore mismatch: $LATEST does not carry the restored code", file=sys.stderr)
        return 5
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan").set_defaults(fn=cmd_plan)
    sub.add_parser("highest-version").set_defaults(fn=cmd_highest)
    r = sub.add_parser("restore")
    r.add_argument("--version", default="")
    r.set_defaults(fn=cmd_restore)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
