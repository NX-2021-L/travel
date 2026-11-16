#!/usr/bin/env python3
"""Verify the DEPLOYED artifact carries every module the repo ships (FLY-TSK-040).

Token-free: `aws lambda get-function` returns a presigned Code.Location for the code that
$LATEST is serving now (call this AFTER apply; it never looks at the local build). Checks:
  1. the zip's top-level .py modules include every repo module (all *.py in the lane
     directory except the deploy-time helpers, the same rule as deploy.sh);
  2. the deployed contract.py declares as many tool names as the repo's contract.py.
This cannot read the served tool list (/caps.json needs a credential); it proves what was
packaged, which is where a dropped module is lost. Exit 1 on any failure.
"""
import os
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

LAMBDA_DIR = Path(__file__).resolve().parent.parent / "backend" / "lambda" / "travel_mcp"
DEPLOY_ONLY_HELPERS = {"resolve_env.py"}  # keep equal to deploy.sh
FUNCTION = os.environ.get("FUNCTION_NAME", "")  # FLY-TSK-028-0B: supplied setting, no default
REGION = os.environ.get("AWS_REGION", "")


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


def check(zip_path, directory=LAMBDA_DIR, extra=""):
    """Return (failure strings, shipped module list); no failures = pass."""
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
    return errors, sorted(shipped)


def main():
    url = subprocess.run(
        ["aws", "lambda", "get-function", "--function-name", FUNCTION, "--region", REGION,
         "--query", "Code.Location", "--output", "text"],
        capture_output=True, text=True, check=True).stdout.strip()
    with tempfile.TemporaryDirectory() as d:
        zp = Path(d) / "deployed.zip"
        urllib.request.urlretrieve(url, zp)  # presigned URL; not echoed
        errors, shipped = check(zp, extra=os.environ.get("DRILL_EXPECT_EXTRA_MODULE", "").strip())
    print(f"deployed modules: {' '.join(shipped)}")
    print(f"expected modules: {' '.join(sorted(expected_modules()))}")
    for e in errors:
        print(f"CHECK FAILED: {e}")
    if not errors:
        print("deployed package OK")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
