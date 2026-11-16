"""FLY-TSK-040: deploy.sh must package every module in backend/lambda/travel_mcp except the
deploy-time helper resolve_env.py, so a lane adding lane_*.py cannot be silently dropped. Offline."""
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAMBDA = ROOT / "backend" / "lambda" / "travel_mcp"
DEPLOY = (LAMBDA / "deploy.sh").read_text()
sys.path.insert(0, str(ROOT / "scripts"))
import verify_deployed_package as vdp  # noqa: E402

EXPECTED = {p.name for p in LAMBDA.glob("*.py")} - {"resolve_env.py"}


def _block():
    m = re.search(r"# BEGIN package-modules\n(.*?)# END package-modules", DEPLOY, re.S)
    assert m, "package-modules markers missing from deploy.sh"
    return m.group(1)


def test_no_single_file_copy():
    assert not re.search(r'^\s*cp\s+"?\$\{SCRIPT_DIR\}/lambda_function\.py', DEPLOY, re.M)


def test_rule_text_globs_all_py_and_excludes_only_resolve_env():
    block = _block()
    assert '"${SCRIPT_DIR}"/*.py' in block
    assert 'DEPLOY_ONLY_HELPERS="resolve_env.py"' in block
    assert vdp.DEPLOY_ONLY_HELPERS == {"resolve_env.py"}


def _run_block(src_dir, build_dir):
    script = (f'set -euo pipefail\nlog(){{ echo "$*"; }}\nSCRIPT_DIR="{src_dir}"\n'
              f'BUILD_DIR="{build_dir}"\n{_block()}')
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


def test_packaging_block_ships_every_module(tmp_path):
    build = tmp_path / "build"
    build.mkdir()
    r = _run_block(LAMBDA, build)
    assert r.returncode == 0, r.stderr
    assert {p.name for p in build.glob("*.py")} == EXPECTED
    assert "contract.py" in EXPECTED
    listed = r.stdout.split("modules:")[1].split()
    assert set(listed) == EXPECTED  # the build log names exactly what shipped


def test_future_lane_module_is_not_dropped(tmp_path):
    src = tmp_path / "src"
    shutil.copytree(LAMBDA, src)
    (src / "lane_2a_records.py").write_text("X = 1\n")
    build = tmp_path / "build"
    build.mkdir()
    assert _run_block(src, build).returncode == 0
    assert (build / "lane_2a_records.py").exists() and not (build / "resolve_env.py").exists()


def test_verify_step_runs_package_check_after_apply():
    wf = (ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text()
    assert wf.index("id: apply") < wf.index("verify_deployed_package.py") < wf.index("id: undo")


def _zip(path, names, contract=None):
    with zipfile.ZipFile(path, "w") as z:
        for n in names:
            z.writestr(n, contract if (n == "contract.py" and contract) else "X = 1\n")
    return path


def test_check_passes_on_full_package(tmp_path):
    z = _zip(tmp_path / "a.zip", EXPECTED | {"pkg/x.py"}, (LAMBDA / "contract.py").read_text())
    errors, _ = vdp.check(z)
    assert errors == []


def test_check_fails_when_contract_dropped(tmp_path):
    errors, _ = vdp.check(_zip(tmp_path / "a.zip", {"lambda_function.py"}))
    assert any("contract.py" in e for e in errors)


def test_check_fails_on_stale_contract(tmp_path):
    errors, _ = vdp.check(_zip(tmp_path / "a.zip", EXPECTED, 'TOOL_NAMES = ("a", "b")\n'))
    assert any("declares 2 tools" in e for e in errors)


def test_check_expected_list_override_fails(tmp_path):
    z = _zip(tmp_path / "a.zip", EXPECTED, (LAMBDA / "contract.py").read_text())
    errors, _ = vdp.check(z, extra="lane_drill.py")
    assert any("lane_drill.py" in e for e in errors)
