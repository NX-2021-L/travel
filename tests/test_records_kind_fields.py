"""FLY-TSK-092 (DVP-PLN-021 lane 2Q): trip_corrections' kind options serve Option.fields.

Each kind names the columns its create needs, matching travel's own per-kind checks, so think's "correct this trip"
panel offers only the field a kind needs. SERVE_OPTION_FIELDS is on since think went live on the W2b kit.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import contract  # noqa: E402
import lane_2e_corrections as corrections  # noqa: E402
import records_routes as rr  # noqa: E402
import records_writes  # noqa: E402


def _kind(d):
    return next(c for c in d["columns"] if c["id"] == "kind")


def test_served_now_think_is_on_the_w2b_kit(monkeypatch):
    assert rr.SERVE_OPTION_FIELDS  # think is live on @io-kit/records 0.7.0 (intelligence 958)
    monkeypatch.setattr(rr, "SERVE_OPTION_FIELDS", False)
    for o in _kind(rr.descriptors()[rr.CORRECTIONS])["options"]:
        assert set(o) == {"value", "label"}


def test_each_kind_serves_its_word_meaning_and_fields(monkeypatch):
    monkeypatch.setattr(rr, "SERVE_OPTION_FIELDS", True)
    cols = {c["id"]: c for c in rr.descriptors()[rr.CORRECTIONS]["columns"]}
    opts = {o["value"]: o for o in cols["kind"]["options"]}
    assert list(opts) == list(contract.CORRECTION_KINDS)
    assert opts["returned"] == {"value": "returned", "label": "returned",
                                "description": "i came home on a date no record shows", "fields": ["date", "record_ids"]}
    assert opts["leave_out"]["label"] == "leave out" and opts["confirmed"]["fields"] == ["record_ids", "first_date", "last_date"]
    named = set()
    for o in opts.values():
        assert o["description"] == o["description"].lower() and o["description"]
        assert len(set(o["fields"])) == len(o["fields"])
        for f in o["fields"]:  # REC-C75/C76: every id is a column of the set, and one this door's create carries
            assert f in cols and cols[f]["editable"] and f in records_writes.CREATE_FIELDS
        named |= set(o["fields"])
    # a column no kind names shows for every kind: only kind (reason is the intent's own field)
    assert {c for c in cols if cols[c]["editable"]} - named == {"kind"}


def test_fields_match_travels_per_kind_checks():
    for kind, fields in rr.KIND_FIELDS.items():
        assert "record_ids" in fields
        assert ("date" in fields) == (kind in ("returned", "left"))
        assert ({"first_date", "last_date"} <= set(fields)) == (kind == "confirmed")


def test_the_panel_offers_no_method_supersedes_or_question():
    cols = {c["id"]: c for c in rr.descriptors()[rr.CORRECTIONS]["columns"]}
    assert not any(cols[k]["editable"] for k in ("method", "supersedes", "question_id"))
    assert records_writes.DOOR_CONFIRMATION_METHOD == "examined"
