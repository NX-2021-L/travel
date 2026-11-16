"""FLY-TSK-028-0B, A-2 (second half): no value of io's instance appears in code, deploy script, workflow
or manifest. The regression guard that keeps the artifact instance-free as later lanes land.

FORBIDDEN is the explicit literal list from finding DOC-05A07958F004 section 1. A deployment's own
values live ONLY in deploy/settings.env (the supplied settings) and in the live environment.

EXCLUDED, deliberately, and why:
  tests/**                       fixtures and test values legitimately pin history (tests/fixtures/
                                 flight_tools_4e0cc8d.json is lane 0A's by-value pin of commit 4e0cc8d)
                                 and play the part of a deployment (tests/conftest.py)
  deploy/settings.env            the ONE supplied-settings file: it is where a deployment is named
  README.md, infra/**, seed/glue/dedup/extract scripts
                                 not code the function runs, not the deploy path; owned by other lanes
                                 or tracked by the finding for later lanes (1C, 3C, 5A)
  caps.json / permission_manifest.json: scanned, but the surface name `io-travel` is ALLOWED in these two
                                 generated snapshots only (they are emitted for one surface; see
                                 scripts/emit_caps.py). Every other forbidden literal is forbidden there.
"""
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TL = ROOT / "backend" / "lambda" / "travel_mcp"

FORBIDDEN = (
    "io-travel-flights", "io-travel-records", "io-travel-mcp-server", "io-travel-mcp-lambda-role",
    "io-travel-edge", "io-travel",
    "thepup.io",
    "356364570033",
    "us-west-2_y6bHKpNZN", "y6bHKpNZN", "6eh4u69l1mqk1m4hfjspeibla8",
    "us-east-1_b2D0V3E1k", "b2D0V3E1k", "41k329h7is7p3p521v9ok96gan",
    "E2DDNP2MQ5IZCS", "wwcp3zgepvpdw2ux26m3mr3i3a0stavm",
    "us-west-2", "us-east-1",
    "finance-mcp",
)

SCANNED = (
    [TL / n for n in ("lambda_function.py", "settings.py", "contract.py", "resolve_env.py", "deploy.sh",
                      "requirements.txt", "caps.json", "permission_manifest.json")]
    + sorted((ROOT / ".github" / "workflows").glob("*.yml"))
    + [ROOT / "scripts" / n for n in ("release_ops.py", "verify_deployed_package.py", "emit_caps.py",
                                      "load_deployment_settings.py", "mcp_post_deploy_probe.sh")]
)

# (file name, literal) pairs allowed; see the module docstring.
ALLOWED = {("caps.json", "io-travel"), ("permission_manifest.json", "io-travel")}


def _hits(path: Path, text: str):
    low = text.lower()
    for lit in FORBIDDEN:
        if (path.name, lit) in ALLOWED:
            continue
        if lit.lower() in low:
            line = next(i for i, l in enumerate(text.splitlines(), 1) if lit.lower() in l.lower())
            yield lit, line


def test_scanned_files_exist():
    missing = [str(p.relative_to(ROOT)) for p in SCANNED if not p.exists()]
    assert not missing, f"no-instance-values scan list names files that do not exist: {missing}"


@pytest.mark.parametrize("path", SCANNED, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_io_instance_value(path):
    found = list(_hits(path, path.read_text()))
    assert not found, "; ".join(
        f"{path.relative_to(ROOT)}:{line} contains forbidden io-instance literal {lit!r}" for lit, line in found
    ) + " -- a value that names a deployment must arrive as a supplied setting (deploy/settings.env), never in the artifact"


def test_every_python_module_the_function_ships_is_scanned_or_named():
    shipped = {p.name for p in TL.glob("*.py")} - {"resolve_env.py"}
    # A module a later lane adds is scanned automatically by this glob, so it cannot slip in an io value.
    for p in TL.glob("*.py"):
        assert not list(_hits(p, p.read_text())), f"{p.relative_to(ROOT)} names an io-instance value"
    assert {"lambda_function.py", "contract.py", "settings.py"} <= shipped


def test_the_guard_actually_detects_a_literal():
    assert ("io-travel-flights", 1) in list(_hits(Path("x.py"), 'T = "io-travel-flights"\n'))
    assert not list(_hits(Path("caps.json"), '{"surface": "io-travel"}'))
    assert list(_hits(Path("caps.json"), '{"table": "io-travel-flights"}'))
