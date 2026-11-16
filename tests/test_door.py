"""FLY-TSK-028-0D, A-4: the internal door, and the refusal of the other door's events in both modes. Offline."""
import json
import pathlib
import sys

import pytest

D = pathlib.Path(__file__).resolve().parent.parent / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import door  # noqa: E402
import resolve_env  # noqa: E402

ROLE = "arn:aws:iam::000000000000:role/test-nana-caller"
HTTP = {"requestContext": {"http": {"method": "POST", "path": "/mcp"}}, "rawPath": "/mcp", "headers": {}}
DIRECT = {"invoker_role": ROLE, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}}


def test_public_edge_admits_http_shaped_event():
    assert door.admit(HTTP, "public", "") == "http"


def test_public_edge_refuses_internal_shaped_event():
    with pytest.raises(door.NonHttpEventRefused):
        door.admit(DIRECT, "public", "")
    with pytest.raises(door.NonHttpEventRefused):
        door.admit({}, "public", "")
    with pytest.raises(door.NonHttpEventRefused):  # an empty http block is not HTTP-shaped
        door.admit({"requestContext": {"http": {}}}, "public", "")


def test_internal_door_admits_direct_event_from_named_role():
    assert door.admit(DIRECT, "internal", ROLE) == "direct"


def test_internal_door_refuses_http_shaped_event():
    with pytest.raises(door.HttpEventRefused):
        door.admit(HTTP, "internal", ROLE)
    with pytest.raises(door.HttpEventRefused):  # even one that also names the role
        door.admit({**DIRECT, **HTTP}, "internal", ROLE)


def test_internal_door_refuses_wrong_or_missing_role_and_no_request():
    for ev in ({**DIRECT, "invoker_role": ROLE + "-other"}, {"mcp": DIRECT["mcp"]},
               {"invoker_role": ROLE}, {"invoker_role": ROLE, "mcp": "x"}, {}):
        with pytest.raises(door.InvokerRefused):
            door.admit(ev, "internal", ROLE)


def test_role_is_a_setting_not_hard_coded():
    assert door.load({"DOOR_MODE": "internal", "INTERNAL_INVOKER_ROLE": ROLE}) == ("internal", ROLE)
    other = ROLE + "-b"
    assert door.admit({**DIRECT, "invoker_role": other}, "internal", other) == "direct"
    with pytest.raises(door.InvokerRefused):
        door.admit(DIRECT, "internal", other)
    assert "arn:aws" not in (D / "door.py").read_text()


def test_mode_fails_closed():
    for env in ({}, {"DOOR_MODE": ""}, {"DOOR_MODE": "both"}, {"DOOR_MODE": "internal"}):
        with pytest.raises(door.DoorError):
            door.load(env)
    assert door.load({"DOOR_MODE": "public"}) == ("public", "")


def test_refusal_is_403_naming_only_the_class():
    r = door.refusal(door.NonHttpEventRefused("x"))
    assert r["statusCode"] == 403 and json.loads(r["body"]) == {"error": "refused", "reason": "NonHttpEventRefused"}


def test_direct_event_becomes_a_request_carrying_the_role():
    ev = door.direct_to_http(DIRECT, ROLE)
    assert ev["requestContext"]["http"]["method"] == "POST"
    assert json.loads(ev["body"]) == DIRECT["mcp"]
    assert ev["io_kit_mcp"]["principal"]["sub"] == ROLE


def test_deploy_guard_refuses_without_mode_and_settings_env_supplies_it():
    import contract
    complete = {k: "x" for k in contract.SETTINGS}
    assert "DOOR_MODE" in resolve_env.guard(complete)
    assert resolve_env.guard({**complete, "DOOR_MODE": "public"}) == ""
    sup = (D.parent.parent.parent / "deploy" / "settings.env").read_text()
    assert "\nDOOR_MODE=public\n" in sup  # io is a public edge
    assert resolve_env.resolve({}, {"DOOR_MODE": "public"})["DOOR_MODE"] == "public"


# ---- the four cases through the real lambda_handler (needs io_kit_mcp_edge: CI only) ----
@pytest.fixture
def lf():
    edge = pytest.importorskip("io_kit_mcp_edge")
    if getattr(edge, "__file__", None) is None:  # another test file installed a stub in sys.modules
        pytest.skip("the real io_kit_mcp_edge is not installed")
    import lambda_function
    return lambda_function


def test_handler_public_http_not_refused(lf, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "public")
    assert lf.lambda_handler(HTTP, None)["statusCode"] == 401  # past the door, the edge asks for a bearer


def test_handler_public_refuses_direct(lf, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "public")
    assert lf.lambda_handler(DIRECT, None)["statusCode"] == 403


def test_handler_internal_refuses_http(lf, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "internal")
    monkeypatch.setattr(lf, "INTERNAL_INVOKER_ROLE", ROLE)
    r = lf.lambda_handler(HTTP, None)
    assert r["statusCode"] == 403 and "HttpEventRefused" in r["body"]


def test_handler_internal_serves_named_role(lf, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "internal")
    monkeypatch.setattr(lf, "INTERNAL_INVOKER_ROLE", ROLE)
    resp = lf.lambda_handler(DIRECT, None)
    assert resp["statusCode"] == 200
    assert "search_flights" in {t["name"] for t in json.loads(resp["body"])["result"]["tools"]}
    assert lf.lambda_handler({**DIRECT, "invoker_role": "other"}, None)["statusCode"] == 403


# ---- LD-39: one health read, read-only; the deploy role gets that and nothing else ----
DEPLOY = "arn:aws:iam::000000000000:role/test-deployer"
PING = {"invoker_role": DEPLOY, "mcp": {"jsonrpc": "2.0", "id": 7, "method": "ping"}}


def test_health_read_answered_to_deploy_role_and_to_invoker():
    assert door.admit(PING, "internal", ROLE, DEPLOY) == "health"
    assert door.admit({**PING, "invoker_role": ROLE}, "internal", ROLE, DEPLOY) == "health"


def test_deploy_role_is_answered_nothing_else():
    for method in ("tools/list", "tools/call", "initialize", ""):
        ev = {"invoker_role": DEPLOY, "mcp": {"jsonrpc": "2.0", "id": 1, "method": method}}
        with pytest.raises(door.InvokerRefused):
            door.admit(ev, "internal", ROLE, DEPLOY)


def test_health_is_refused_to_an_unnamed_role_and_in_public_mode():
    with pytest.raises(door.InvokerRefused):
        door.admit({**PING, "invoker_role": "other"}, "internal", ROLE, DEPLOY)
    with pytest.raises(door.InvokerRefused):  # no deploy role configured: the claim matches nothing
        door.admit(PING, "internal", ROLE, "")
    with pytest.raises(door.NonHttpEventRefused):
        door.admit(PING, "public", "", DEPLOY)


def test_health_answer_is_minimal_and_carries_no_person_data():
    out = door.health(PING, "some-surface")
    assert out == {"jsonrpc": "2.0", "id": 7, "result": {"status": "ok", "door": "internal", "surface": "some-surface"}}


def test_health_does_no_io():
    src = (D / "door.py").read_text()
    assert "boto3" not in src and "urllib" not in src and "requests" not in src


def test_the_role_claim_is_a_tripwire_not_an_authorization_boundary():
    # Documented in door.py and asserted here so nobody reads the refusals above as isolation: Lambda does not
    # attest the caller, so a caller able to invoke can assert any role string. The resource policy is the gate.
    assert "DECLARED CLAIM" in (D / "door.py").read_text()
    assert door.admit({"invoker_role": ROLE, "mcp": {"method": "tools/list"}}, "internal", ROLE) == "direct"


def test_handler_health_read(lf, monkeypatch):
    monkeypatch.setattr(lf, "DOOR_MODE", "internal")
    monkeypatch.setattr(lf, "INTERNAL_INVOKER_ROLE", ROLE)
    monkeypatch.setattr(lf, "INTERNAL_DEPLOY_ROLE", DEPLOY)
    out = lf.lambda_handler(PING, None)
    assert out["result"]["status"] == "ok" and out["id"] == 7
    r = lf.lambda_handler({"invoker_role": DEPLOY, "mcp": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}}, None)
    assert r["statusCode"] == 403
