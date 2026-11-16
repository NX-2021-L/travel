#!/usr/bin/env python3
"""Verify the DEPLOYED artifact is the build, byte for byte (FLY-TSK-040, extended by FLY-TSK-031-0A, A-14).

Token-free: `aws lambda get-function` returns a presigned Code.Location for the code that
$LATEST is serving now (call this AFTER apply; it never looks at the local build). Checks:
  1. the zip's top-level .py modules include every repo module (all *.py in the lane
     directory except the deploy-time helpers, the same rule as deploy.sh);
  2. every one of those modules is BYTE-IDENTICAL to the repo's (the tool list and schemas are a
     function of this code alone, so identical code builds an identical list);
  3. the shipped caps.json and permission_manifest.json are byte-identical to the repo's, which
     are what emit_caps.py renders (those two files name no deployment);
  4. the DEPLOYED code, executed, renders those same bytes (generated_differences): the tool list
     and schemas the deployed build produces equal the repo's;
  5. the downloaded zip is the active package: its SHA-256 equals Configuration.CodeSha256;
  6. the deployed contract.py declares as many tool names as the repo's contract.py.
NOT proven here: that the running function's /mcp tools/list response equals the artifact. /caps.json
and tools/list need a credential this job does not hold (the public edge answers 401); the step from
"$LATEST holds this code" to "the process answers from it" is Lambda's, not checked by bytes.
Exit 1 on any failure.
"""
import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

LAMBDA_DIR = Path(__file__).resolve().parent.parent / "backend" / "lambda" / "travel_mcp"
DEPLOY_ONLY_HELPERS = {"resolve_env.py"}  # keep equal to deploy.sh
SNAPSHOTS = ("caps.json", "permission_manifest.json")  # keep equal to deploy.sh
FUNCTION = os.environ.get("FUNCTION_NAME", "")  # FLY-TSK-028-0B: supplied setting, no default
REGION = os.environ.get("AWS_REGION", "")

# Executed against the EXTRACTED deployed code: renders the two documents exactly as scripts/emit_caps.py does.
_RENDER = """
import json, os, sys
sys.path.insert(0, sys.argv[1])
import contract
for k, s in contract.SETTINGS.items():
    if s["required"]:
        os.environ.setdefault(k, "verify-marker")
os.environ.setdefault("DOOR_MODE", "public")
os.environ.setdefault("AWS_REGION", "verify-marker")
import lambda_function as lf
def r(d):
    return json.dumps(d, indent=2, sort_keys=True) + "\\n"
json.dump({"caps.json": r(lf.build_caps()), "permission_manifest.json": r(lf.build_permission_manifest())}, sys.stdout)
"""


def expected_modules(directory=LAMBDA_DIR):
    return {p.name for p in Path(directory).glob("*.py")} - DEPLOY_ONLY_HELPERS


def zip_modules(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        return {n for n in z.namelist() if "/" not in n and n.endswith(".py")}


def tool_count(contract_source):
    """Number of names in TOOL_NAMES of a contract.py source (executed in isolation)."""
    out = subprocess.run(
        [sys.executable, "-I", "-c",
         "import sys; ns={'__name__':'contract'}; exec(compile(sys.stdin.read(),'contract.py','exec'),ns);"
         "print(len(ns['TOOL_NAMES']))"],
        input=contract_source, capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def byte_differences(zip_path, directory=LAMBDA_DIR):
    """Repo modules/snapshots whose bytes in the zip differ from the repo's, or snapshots not shipped."""
    out = []
    with zipfile.ZipFile(zip_path) as z:
        names = set(z.namelist())
        for n in sorted(expected_modules(directory) | set(SNAPSHOTS)):
            if n not in names:
                if n in SNAPSHOTS:
                    out.append(f"{n} (not shipped)")
                continue  # a missing module is reported by check()
            if z.read(n) != (Path(directory) / n).read_bytes():
                out.append(n)
    return out


def generated_differences(zip_path, directory=LAMBDA_DIR):
    """Run the DEPLOYED code and compare what it renders with the repo's snapshots, byte for byte."""
    with tempfile.TemporaryDirectory() as d:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(d)
        pypath = os.pathsep.join(p for p in (d, os.environ.get("PYTHONPATH", "")) if p)
        run = subprocess.run([sys.executable, "-c", _RENDER, d], capture_output=True, text=True,
                             env={**os.environ, "PYTHONPATH": pypath}, cwd=d)
        if run.returncode != 0:
            tail = (run.stderr.strip().splitlines() or ["no output"])[-1]
            return [f"the deployed code could not be executed to render its tool list: {tail}"]
        rendered = json.loads(run.stdout)
    return [f"deployed code renders a different {n} than the repo's" for n in SNAPSHOTS
            if rendered[n] != (Path(directory) / n).read_text()]


def check(zip_path, directory=LAMBDA_DIR, extra=""):
    """Return (failure strings, shipped module list); no failures = pass. Pure: executes nothing deployed."""
    errors = []
    expected = expected_modules(directory) | ({extra} if extra else set())
    shipped = zip_modules(zip_path)
    missing = sorted(expected - shipped)
    if missing:
        errors.append(f"deployed package is missing modules: {missing}")
    if "contract.py" in shipped:
        with zipfile.ZipFile(zip_path) as z:
            got = tool_count(z.read("contract.py").decode())
        want = tool_count((Path(directory) / "contract.py").read_text())
        if got != want:
            errors.append(f"deployed contract.py declares {got} tools, repo declares {want}")
    differ = byte_differences(zip_path, directory)
    if differ:
        errors.append(f"deployed bytes differ from the repo's: {differ}")
    return errors, sorted(shipped)


def main():
    info = json.loads(subprocess.run(
        ["aws", "lambda", "get-function", "--function-name", FUNCTION, "--region", REGION,
         "--query", "{loc: Code.Location, sha: Configuration.CodeSha256}", "--output", "json"],
        capture_output=True, text=True, check=True).stdout)
    with tempfile.TemporaryDirectory() as d:
        zp = Path(d) / "deployed.zip"
        urllib.request.urlretrieve(info["loc"], zp)  # presigned URL; not echoed
        errors, shipped = check(zp, extra=os.environ.get("DRILL_EXPECT_EXTRA_MODULE", "").strip())
        actual = base64.b64encode(hashlib.sha256(zp.read_bytes()).digest()).decode()
        if actual != info["sha"]:
            errors.append("the downloaded package is not the active one (sha256 differs from Configuration.CodeSha256)")
        if not errors:  # a package that already differs from the repo needs no execution to be refused
            errors += generated_differences(zp)
    print(f"deployed modules: {' '.join(shipped)}")
    print(f"expected modules: {' '.join(sorted(expected_modules()))}")
    for e in errors:
        print(f"CHECK FAILED: {e}")
    if not errors:
        print("deployed package OK: modules and snapshots byte-identical to the repo, deployed code renders the same tool list")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
