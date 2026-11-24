"""FLY-TSK-092 (DVP-PLN-021 lane 2Q): trip_corrections' kind options serve Option.fields.

Each kind names the columns its create needs, matching travel's own per-kind checks, so think's "correct this trip"
panel offers only the field a kind needs. Served behind SERVE_OPTION_FIELDS until think is live on the W2b kit.
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


def test_off_until_the_w2b_kit_is_live():
    for o in _kind(rr.descriptors()[rr.CORRECTIONS])["options"]:
        assert set(o) == {"value", "label"}


def test_each_kind_serves_its_fields_with_the_gloss_as_label(monkeypatch):
    monkeypatch.setattr(rr, "SERVE_OPTION_FIELDS", True)
    cols = {c["id"]: c for c in rr.descriptors()[rr.CORRECTIONS]["columns"]}
    opts = {o["value"]: o for o in cols["kind"]["options"]}
    assert list(opts) == list(contract.CORRECTION_KINDS)
    assert opts["returned"] == {"value": "returned", "label": "i came home on a date no record shows",
                                "fields": ["date", "record_ids"]}
    assert all("description" not in o for o in opts.values())  # the board draws none
    named = set()
    for o in opts.values():
        assert len(set(o["fields"])) == len(o["fields"])
        for f in o["fields"]:  # every id is a column of the set, and one a create carries
            assert f in cols and cols[f]["editable"] and f in records_writes.CREATE_FIELDS
        named |= set(o["fields"])
    # a column no kind names shows for every kind: only kind and reason
    assert {c for c in cols if cols[c]["editable"]} - named == {"kind"}


def test_fields_match_travels_per_kind_checks():
    for kind, fields in rr.KIND_FIELDS.items():
        assert "record_ids" in fields
        assert ("date" in fields) == (kind in ("returned", "left"))
        confirmed_only = set(corrections._CONFIRMED_ONLY)
        assert (confirmed_only <= set(fields)) if kind == "confirmed" else not (confirmed_only & set(fields))
