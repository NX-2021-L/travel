"""FLY-TSK-027-0A: plan output, restore argument construction, deploy-path structure. Offline."""
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
import release_ops as ro  # noqa: E402
import resolve_env  # noqa: E402

WF = yaml.safe_load((ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text())
ON = WF.get("on", WF.get(True))


def test_diff_env_keys_only():
    d = resolve_env.diff_env({"A": "1", "B": "2", "C": "3"}, {"A": "1", "B": "9", "D": "x"})
    assert d == [("B", "changed"), ("C", "removed"), ("D", "added")]


def test_resolve_env_plan_never_prints_values(monkeypatch, capsys):
    monkeypatch.setenv("CURRENT_ENV", '{"TABLE_NAME":"old-secret-value","COGNITO_CLIENT_SECRET":"zzz"}')
    monkeypatch.setattr(sys, "argv", ["resolve_env.py", "--plan"])
    for k in resolve_env.KEYS:  # a complete deployment: FLY-TSK-028-0B makes every required setting mandatory
        monkeypatch.setenv(k, "supplied-" + k.lower())
    assert resolve_env.main() == 0
    out = capsys.readouterr().out
    assert "old-secret-value" not in out and "zzz" not in out
    assert "removed  COGNITO_CLIENT_SECRET" in out and "added    MCP_TRANSPORT" in out


def test_highest_version_ignores_latest():
    v = {"Versions": [{"Version": "$LATEST"}, {"Version": "3"}, {"Version": "12"}]}
    assert ro.highest_version(v) == 12
    assert ro.highest_version({"Versions": [{"Version": "$LATEST"}]}) == 0


def test_render_plan_contents():
    cfg = {"CodeSha256": "abc=", "LastModified": "2026-01-01T00:00:00.000+0000"}
    out = ro.render_plan(cfg, 4, [("X", "added")], ["boto3"], region="r-1", function="io-travel-mcp-server")
    for needle in ("io-travel-mcp-server", "abc=", "2026-01-01", "highest published version: 4", "added    X", "boto3"):
        assert needle in out


def test_restore_args():
    cfg = {"Runtime": "python3.12", "Handler": "h.f", "MemorySize": 512, "Timeout": 30}
    a = ro.restore_config_args(cfg, "/tmp/e.json")
    assert a[:4] == ["lambda", "update-function-configuration", "--function-name", ro.FUNCTION]
    assert "file:///tmp/e.json" in a and a[a.index("--memory-size") + 1] == "512"
    c = ro.restore_code_args("/tmp/z.zip")
    assert c[-2:] == ["--zip-file", "fileb:///tmp/z.zip"]


def test_paths_filter_unchanged():
    """A merge touching nothing of travel's deploys nothing of travel's."""
    assert ON["push"]["paths"] == ["backend/lambda/travel_mcp/**", ".github/workflows/lambda-travel-mcp-deploy.yml"]
    assert ON["push"]["branches"] == ["main"]


def test_deploy_path_has_four_steps_in_order():
    names = [s["name"] for s in WF["jobs"]["release"]["steps"]]
    idx = {k: next(i for i, n in enumerate(names) if n.startswith(k)) for k in ("Plan", "Apply", "Verify", "Undo")}
    assert idx["Plan"] < idx["Apply"] < idx["Verify"] < idx["Undo"]
    assert "rollback_to_version" in ON["workflow_dispatch"]["inputs"]
    assert "force_verify_fail" in ON["workflow_dispatch"]["inputs"]


def test_deploys_are_serialized_and_never_cancelled():
    assert WF["concurrency"] == {"group": "travel-mcp-deploy", "cancel-in-progress": False}


def test_records_table_name_defaults_and_survives_merge():
    # FLY-TSK-041, tightened by FLY-TSK-028-0B: no default; supplied or live, never invented.
    assert "RECORDS_TABLE_NAME" not in resolve_env.resolve({}, {})
    assert resolve_env.resolve({"RECORDS_TABLE_NAME": "live"}, {})["RECORDS_TABLE_NAME"] == "live"
    assert resolve_env.resolve({"RECORDS_TABLE_NAME": "live"}, {"RECORDS_TABLE_NAME": "new"})["RECORDS_TABLE_NAME"] == "new"
