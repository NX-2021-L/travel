"""FLY-TSK-088 (BRD DOC-D14184D324F0 6.2, 6.7): descriptor content and the corpus runner in the pipeline."""
from pathlib import Path

import pytest

import records_fixture as fx

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def routes(monkeypatch):
    for k, v in fx.env().items():
        monkeypatch.setenv(k, v)
    fx.install(monkeypatch)
    import records_routes
    return records_routes


def test_every_enum_column_serves_inline_options_from_the_contract(routes):
    import contract
    import trip_engine
    want = {"kind": contract.CORRECTION_KINDS, "status": contract.CORRECTION_STATUSES, "method": contract.CONFIRMATION_METHODS,
            "first_basis": contract.BASES, "last_basis": contract.BASES, "state": tuple(trip_engine.STATE_ORDER)}
    seen = {}
    for d in routes.descriptors().values():
        for c in d["columns"]:
            if c["type"] == "enum":
                assert [o["value"] for o in c["options"]] == list(want[c["id"]]), c["id"]
                assert all(isinstance(o["label"], str) and o["label"] for o in c["options"])
                seen[c["id"]] = c
    assert set(seen) == set(want)


def test_kind_labels_are_travels_own_glosses(routes):
    kind = next(c for c in routes.descriptors()["trip_corrections"]["columns"] if c["id"] == "kind")
    labels = {o["value"]: o["label"] for o in kind["options"]}
    assert labels["returned"] == "i came home on a date no record shows"
    assert labels["leave_out"] == "this is not a trip"
    assert all(label == label.lower() for label in labels.values())  # lowercase is in the strings, never CSS


def test_a_vocabulary_value_without_a_gloss_fails_loudly(routes, monkeypatch):
    monkeypatch.setitem(routes._VALUES, "kind", routes._VALUES["kind"] + ("new_kind",))
    with pytest.raises(RuntimeError, match="do not match the contract"):
        routes._options("kind")


def test_both_descriptors_carry_a_description(routes):
    for d in routes.descriptors().values():
        assert isinstance(d["description"], str) and d["description"].strip()


def test_the_read_groups_run_in_ci_on_the_published_kit_in_read_only_mode():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    runner = (ROOT / "tests" / "conformance" / "run_records_conformance.mjs").read_text()
    assert '"@io-kit/records@0.4.0"' in ci and "/npm/${CODEARTIFACT_REPOSITORY}/" in ci
    assert "node tests/conformance/run_records_conformance.mjs" in ci
    assert "readOnly: true" in runner and "process.exit(bad ? 1 : 0)" in runner
