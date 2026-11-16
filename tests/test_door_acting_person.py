"""FLY-TSK-032-0B (lane 3D), A-30/A-31 at the door module: the acting person and the session on the direct event.

Stdlib + pytest only, so this file runs on every pull request, including the limited PR-time CI that cannot install
io-kit-mcp-edge (see .github/workflows/ci.yml). The handler-level proofs are in test_acting_person_handler.py.
"""
import json
import pathlib
import sys

import pytest

D = pathlib.Path(__file__).resolve().parent.parent / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import door  # noqa: E402

ROLE = "arn:aws:iam::000000000000:role/test-nana-caller"
DEPLOY = "arn:aws:iam::000000000000:role/test-deployer"
CALL = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "create_lodging", "arguments": {}}}


def ev(**kw):
    return {"invoker_role": ROLE, "mcp": CALL, **kw}


@pytest.mark.parametrize("good", ["nana", "io", "scheduler", "a.b_c:d-e", "A" * 128, "0", "user:123"])
def test_valid_ids_are_accepted(good):
    assert door.acting(ev(acting_principal=good, session=good), ROLE, DEPLOY) == (good, good)


@pytest.mark.parametrize("bad", [None, "", " ", "a b", "nana@example.com", "a/b", "A" * 129, "nana\n", "\nnana", "é",
                                 7, 1.5, True, ["nana"], {"id": "nana"}, b"nana"])
def test_anything_else_counts_as_absent_and_never_raises(bad):
    assert door.acting(ev(acting_principal=bad, session=bad), ROLE, DEPLOY) == ("", "")


def test_a_role_valued_principal_is_absent_not_an_error():
    assert door.acting(ev(acting_principal=ROLE), ROLE, DEPLOY) == ("", "")
    assert door.acting(ev(acting_principal=DEPLOY), ROLE, DEPLOY) == ("", "")
    # a role-valued id that happens to match the pattern is still a role
    assert door.acting(ev(acting_principal="r.o:le"), "r.o:le", "") == ("", "")


def test_keys_are_optional_and_read_only_at_the_top_level():
    assert door.acting(ev(), ROLE) == ("", "")
    nested = ev(mcp={**CALL, "acting_principal": "x", "session": "y",
                     "params": {**CALL["params"], "acting_principal": "x"}}, io_kit_mcp={"principal": {"acting_principal": "x"}})
    assert door.acting(nested, ROLE) == ("", "")


def test_admission_never_reads_the_acting_person():
    # same verdict whatever the two keys hold, including values that would be refused as a person
    for extra in ({}, {"acting_principal": "nana"}, {"acting_principal": ROLE}, {"acting_principal": 7, "session": []},
                  {"acting_principal": {"x": 1}}):
        assert door.admit(ev(**extra), "internal", ROLE, DEPLOY) == "direct"
    src = (D / "door.py").read_text()
    admit_src = src[src.index("def admit("):src.index("def health(")]
    assert "acting_principal" not in admit_src and "session" not in admit_src


@pytest.mark.parametrize("claimed", [["x"], {"a": 1}, 7, 1.5, True, b"x", ("x",), [ROLE], {ROLE: 1}])
def test_a_non_string_invoker_role_is_refused_and_does_not_raise(claimed):
    for mode_role in (ROLE, "x"):
        with pytest.raises(door.InvokerRefused):
            door.admit({"invoker_role": claimed, "mcp": CALL}, "internal", mode_role, DEPLOY)
    with pytest.raises(door.InvokerRefused):
        door.admit({"invoker_role": claimed, "mcp": {"method": "ping"}}, "internal", ROLE, DEPLOY)


def test_direct_to_http_carries_the_two_labels_and_the_role_stays_the_principal_sub():
    out = door.direct_to_http(ev(acting_principal="nana", session="s-1"), ROLE, DEPLOY)
    assert out["io_kit_mcp"]["principal"] == {"sub": ROLE, "groups": [], "acting_principal": "nana", "session": "s-1"}
    assert out["io_kit_mcp"]["pool"] == "internal-door" and json.loads(out["body"]) == CALL
    out = door.direct_to_http(ev(acting_principal=ROLE, session="bad id"), ROLE, DEPLOY)
    assert out["io_kit_mcp"]["principal"]["acting_principal"] == "" and out["io_kit_mcp"]["principal"]["session"] == ""


def test_the_door_module_stays_stdlib_only():
    assert "boto3" not in (D / "door.py").read_text()
