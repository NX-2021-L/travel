"""MOM-TSK-021: every resource name comes from a deployment's own settings file, and nana's are all nana-*.

io's ruling (2026-11-19): every resource in nana's AWS account is named `nana-*`, fixed durably in the code so that
nothing is hard-coded. One naming source per deployment target (io: deploy/settings.env, hers: deploy/targets/nana.env);
the template, workflow, scripts and function derive every name from it. This guard fails the build when:

  1. a literal `io-travel` appears in any file of the repository outside the allowlist below (a resource name formed
     in a script, a workflow, the template, a default argument ... is the thing that put io-* names in her account);
  2. her target file names a resource that is not `nana-*`, or carries `io-travel` anywhere;
  3. io's own names drift from the values they have always had (renaming io's resources must be a decision, not a
     side effect of this change; a rename here would replace io's tables).

ALLOWED, deliberately, and why:
  deploy/settings.env        the supplied-settings file of io's deployment: it is where io's resources are named
  tests/**                   fixtures play the part of a deployment and pin history
  docs/**, README.md         prose (the connector's product name), no resource is formed from it
  infra/cloudfront/**        io's CloudFront Lambda@Edge (us-east-1, io's public edge only); her deployment has no
                             CloudFront and never deploys these two files
"""
import os
from pathlib import Path

import pytest

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import load_deployment_settings as lds  # noqa: E402

LITERAL = "io-travel"
SKIP_DIRS = {".git", "__pycache__", "node_modules", ".pytest_cache", ".venv", "venv", ".mypy_cache"}
ALLOWED_PREFIXES = ("deploy/settings.env", "tests/", "docs/", "README.md", "infra/cloudfront/")

IO = lds.parse((ROOT / "deploy/settings.env").read_text())
HERS = lds.parse((ROOT / "deploy/targets/nana.env").read_text())

# io's names as they stand in production today. They may change only by a deliberate, reviewed rename.
IO_NAMES = {
    "TABLE_NAME": "io-travel-flights",
    "RECORDS_TABLE_NAME": "io-travel-records",
    "FUNCTION_NAME": "io-travel-mcp-server",
    "SURFACE_NAME": "io-travel",
}
HER_NAME_KEYS = ("STACK_NAME", "DEPLOY_ROLE_NAME", "TABLE_NAME", "RECORDS_TABLE_NAME", "FUNCTION_NAME", "LAMBDA_ROLE_NAME")


def _files():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for n in filenames:
            p = Path(dirpath) / n
            rel = p.relative_to(ROOT).as_posix()
            if rel.startswith(ALLOWED_PREFIXES):
                continue
            yield p, rel


def test_no_literal_io_travel_outside_the_settings_of_io_and_the_allowlist():
    found = []
    for p, rel in _files():
        try:
            text = p.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if LITERAL in line.lower():
                found.append(f"{rel}:{i}")
    assert not found, (
        "literal 'io-travel' where a name is formed: " + ", ".join(found)
        + " -- a resource name arrives from the deployment's settings file (deploy/settings.env, deploy/targets/*.env), never from code"
    )


@pytest.mark.parametrize("key", HER_NAME_KEYS)
def test_her_resource_names_are_all_nana(key):
    assert HERS[key].startswith("nana-"), f"deploy/targets/nana.env {key}={HERS[key]!r} must start with nana-"


def test_her_target_file_names_no_io_travel_anywhere():
    for k, v in HERS.items():
        assert LITERAL not in v.lower(), f"deploy/targets/nana.env {k} carries io-travel"


def test_her_names_are_distinct_from_io_names():
    for key in ("TABLE_NAME", "RECORDS_TABLE_NAME", "FUNCTION_NAME"):
        assert HERS[key] != IO[key], key


def test_io_names_are_unchanged():
    for k, v in IO_NAMES.items():
        assert IO[k] == v, f"io's {k} changed from {v!r}: renaming io's resources replaces them; it is not part of MOM-TSK-021"
    assert IO["LAMBDA_ROLE_ARN"].endswith(":role/io-travel-mcp-lambda-role")
