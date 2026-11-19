"""ENC-ISS-919: io-kit ships no fleet registry; io's public deployment supplies its own through settings, hers supplies none."""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
sys.path.insert(0, str(ROOT / "scripts"))

import resolve_env  # noqa: E402
import contract  # noqa: E402
from load_deployment_settings import parse  # noqa: E402

IO = parse((ROOT / "deploy" / "settings.env").read_text())
NANA = parse((ROOT / "deploy" / "targets" / "nana.env").read_text())


def test_io_settings_carry_a_registry_for_exactly_its_surface():
    reg = json.loads(IO["IO_KIT_FLEET_JSON"])
    assert [c["name"] for c in reg["connectors"]] == [IO["SURFACE_NAME"]]
    assert set(reg["profiles"]) == {reg["connectors"][0]["profile"]}


def test_her_target_supplies_no_registry():
    assert "IO_KIT_FLEET_JSON" not in NANA
    text = (ROOT / "deploy" / "targets" / "nana.env").read_text().lower()
    assert "thepup" not in text and "jreese" not in text


def test_registry_setting_reaches_the_function_env_and_is_not_a_contract_key():
    assert "IO_KIT_FLEET_JSON" not in contract.SETTINGS
    env = resolve_env.resolve({}, {"IO_KIT_FLEET_JSON": "{}", "DOOR_MODE": "public"})
    assert env["IO_KIT_FLEET_JSON"] == "{}"
    assert "IO_KIT_FLEET_JSON" not in resolve_env.resolve({}, {"DOOR_MODE": "internal"})


def test_public_deploy_refuses_without_registry_internal_does_not():
    assert "IO_KIT_FLEET_JSON" in resolve_env.fleet_problem({"DOOR_MODE": "public"})
    assert "IO_KIT_FLEET_JSON" in resolve_env.fleet_problem({})  # an unknown door reads as public
    assert resolve_env.fleet_problem({"DOOR_MODE": "public", "IO_KIT_FLEET_JSON": "{}"}) == ""
    assert resolve_env.fleet_problem({"DOOR_MODE": "internal"}) == ""


def test_io_registry_resolves_through_the_kit_when_installed(monkeypatch):
    kit = __import__("pytest").importorskip("io_kit_mcp_edge")
    monkeypatch.setenv("IO_KIT_FLEET_JSON", IO["IO_KIT_FLEET_JSON"])
    kit.set_fleet(None)
    r = kit.resolve_connector(IO["SURFACE_NAME"])
    assert r["mount_path"] == "/mcp" and re.match(r"^https://", r["base_url"]) and r["client_ids"]
