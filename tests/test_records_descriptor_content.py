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
    want = {"kind": contract.CORRECTION_KINDS, "method": contract.CONFIRMATION_METHODS,
            "first_basis": contract.BASES, "last_basis": contract.BASES, "state": tuple(trip_engine.STATE_ORDER)}
    seen = {}
    for d in routes.descriptors().values():
        for c in d["columns"]:
            if c["type"] == "enum":
                assert [o["value"] for o in c["options"]] == list(want[c["id"]]), c["id"]
                assert all(isinstance(o["label"], str) and o["label"] for o in c["options"])
                seen[c["id"]] = c
    assert set(seen) == set(want)


def test_kind_options_carry_travels_own_words_and_glosses(routes):
    kind = next(c for c in routes.descriptors()["trip_corrections"]["columns"] if c["id"] == "kind")
    opts = {o["value"]: o for o in kind["options"]}
    assert opts["returned"]["label"] == "returned" and opts["leave_out"]["label"] == "leave out"  # canvas A7.7
    assert opts["returned"]["description"] == "i came home on a date no record shows"
    assert opts["leave_out"]["description"] == "this is not a trip"
    for o in opts.values():  # lowercase is in the strings, never CSS
        assert o["label"] == o["label"].lower() and o["description"] == o["description"].lower()


def test_a_vocabulary_value_without_a_gloss_fails_loudly(routes, monkeypatch):
    monkeypatch.setitem(routes._VALUES, "kind", routes._VALUES["kind"] + ("new_kind",))
    with pytest.raises(RuntimeError, match="do not match the contract"):
        routes._options("kind")


def test_both_descriptors_carry_a_description(routes):
    for d in routes.descriptors().values():
        assert isinstance(d["description"], str) and d["description"].strip()


def test_the_corpus_gate_runs_in_ci_and_before_the_deploy_changes_anything():
    """FLY-TSK-088 read groups and FLY-TSK-089 write groups: one script, on the published kit, a required gate."""
    import json
    import re
    import yaml
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    script = (ROOT / "scripts" / "records_conformance.sh").read_text()
    reads = (ROOT / "tests" / "conformance" / "run_records_conformance.mjs").read_text()
    writes = (ROOT / "tests" / "conformance" / "run_records_conformance_writes.mjs").read_text()
    assert re.fullmatch(r"\d+\.\d+\.\d+", (ROOT / "tests" / "conformance" / "KIT_VERSION").read_text().strip())
    assert "/npm/${CODEARTIFACT_REPOSITORY}/" in script and '"@io-kit/records@${VERSION}"' in script
    assert "run_records_conformance.mjs" in script and "run_records_conformance_writes.mjs" in script and "--writable" in script
    assert "readOnly: true" in reads and "readOnly: false" in writes and "process.exit(bad ? 1 : 0)" in writes
    assert "bash scripts/records_conformance.sh" in ci
    release = yaml.safe_load((ROOT / ".github" / "workflows" / "lambda-travel-mcp-deploy.yml").read_text())["jobs"]["release"]
    names = [st.get("name", "") for st in release["steps"]]
    gate = next(i for i, n in enumerate(names) if n.startswith("Records conformance gate"))
    assert gate < next(i for i, n in enumerate(names) if n.startswith("Plan")) < next(i for i, n in enumerate(names) if n.startswith("Apply"))
    assert "bash scripts/records_conformance.sh" in release["steps"][gate]["run"] and "continue-on-error" not in release["steps"][gate]
    skipped = json.loads((ROOT / "tests" / "conformance" / "not_declared.json").read_text())
    assert all(isinstance(v, str) and v.strip() for k, v in skipped.items() if k.startswith("REC-"))
