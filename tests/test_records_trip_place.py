"""FLY-TSK-091 (DVP-PLN-021 lane 2N): trips serve a place label and the paired-cell hints the canvas draws.

The label is drawn from the trip's own destination records: a stay's city, a drive's destination, a rental's pickup
place, else the place id read as words; "unknown" on the records door when the trip visited no place. Column.hint.under
(the W2b kit release) pairs the key under the place, each basis under its date and nights_exact under nights.
Synthetic values only.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
sys.path.insert(0, str(ROOT / "tests"))
import trip_engine as te  # noqa: E402
from test_trip_engine import BOS, DEN, HOME1, JFK, SEA, SFO, PDX, drive, fl, lodge, run  # noqa: E402,F401


def _trip(recs):
    trips = run(recs)["trips"]
    assert len(trips) == 1
    return trips[0]


def test_a_stay_names_its_city_over_the_flight_that_reached_it():
    t = _trip([fl("f1", "2024-03-01", SEA, JFK), lodge("l1", "2024-03-01", "2024-03-03", JFK, city="Gotham"),
               fl("f2", "2024-03-04", JFK, SEA)])
    assert t["places_visited"] == ["city:new-york"] and t["place"] == "Gotham"


def test_a_flight_alone_reads_its_area_as_words_and_connections_are_not_named():
    t = _trip([fl("f1", "2024-03-01", SEA, DEN), fl("f2", "2024-03-01", DEN, SFO), fl("f3", "2024-03-05", SFO, SEA)])
    assert t["place"] == "san francisco"


def test_an_airport_with_no_area_reads_its_code():
    t = _trip([fl("f1", "2024-03-01", SEA, ("apt:XYZ", None)), fl("f2", "2024-03-05", ("apt:XYZ", None), SEA)])
    assert t["place"] == "XYZ"


def test_a_drive_names_its_destination():
    t = _trip([dict(drive("d1", "2024-03-01", SEA, PDX), destination="Rose Town"),
               dict(drive("d2", "2024-03-04", PDX, SEA), destination="Home Town")])
    assert t["place"] == "Rose Town"


def test_places_are_listed_in_order_and_past_three_end_and_n_more():
    stops = [("apt:AAA", "city:alpha"), ("apt:BBB", "city:beta"), ("apt:CCC", "city:gamma"), ("apt:DDD", "city:delta")]
    recs, prev = [], SEA
    for i, p in enumerate(stops):
        recs.append(fl(f"f{i}", f"2024-03-0{2 * i + 1}", prev, p))
        prev = p
    recs.append(fl("fz", "2024-03-09", prev, SEA))
    t = _trip(recs)
    assert t["place"] == "alpha, beta, gamma and 1 more"
    assert te.PLACE_LABEL_MAX == 3


def test_the_same_words_twice_are_named_once():
    t = _trip([fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-03", JFK, BOS), fl("f3", "2024-03-05", BOS, JFK),
               fl("f4", "2024-03-07", JFK, SEA)])
    assert t["place"] == "new york, boston"


# --------------------------------------------------------------------------- the records door
import records_routes as rr  # noqa: E402


def _cols(d):
    return {c["id"]: c for c in d["columns"]}


def test_the_place_column_leads_and_unknown_is_served_when_none():
    d = rr.descriptors()[rr.TRIPS]
    assert [c["id"] for c in d["columns"]][:2] == ["place", "key"] and d["key"] == "key"
    assert _cols(d)["place"]["label"] == "trip" and _cols(d)["place"]["type"] == "text"
    row = rr._trip_row({"key": "k1", "place": None, "record_ids": []})
    assert row["values"]["place"] == rr.PLACE_UNKNOWN == "unknown"
    assert rr._trip_row({"key": "k1", "place": "Gotham", "record_ids": []})["values"]["place"] == "Gotham"


def test_the_source_input_label_is_lowercase():
    assert rr.descriptors()[rr.TRIPS]["inputs"][0] == {"external": "travel_records", "label": "travel records"}


def test_corrections_show_the_boards_columns_and_hide_the_rest():
    cols = rr.descriptors()[rr.CORRECTIONS]["columns"]
    shown = [c["id"] for c in cols if not c.get("hint", {}).get("default_hidden")]
    assert shown == ["kind", "date", "reason", "meta"]  # io 2026-11-25: no id column; the kit's copy button has it
    ids = {c["id"]: c for c in cols}
    assert ids["correction_id"]["hint"] == {"default_hidden": True}  # no width: the board's 232 px went with the column
    assert ids["kind"]["hint"]["default_width"] == 104 and "default_width" not in ids["reason"].get("hint", {})
    assert {c["id"] for c in cols} >= {"author", "session", "record_ids", "method", "supersedes"}


def test_under_hints_are_served_now_think_is_on_the_w2b_kit(monkeypatch):
    assert rr.SERVE_UNDER_HINTS  # think is live on @io-kit/records 0.7.0 (intelligence 958)
    monkeypatch.setattr(rr, "SERVE_UNDER_HINTS", False)
    off = _cols(rr.descriptors()[rr.TRIPS])
    assert not any("under" in c.get("hint", {}) or "tone" in c.get("hint", {}) for c in off.values())
    assert not any(off[k].get("hint", {}).get("default_hidden") for k in ("key", *rr.HIDDEN_WHEN_HINTED))
    assert all(off[k]["hint"]["default_hidden"] for k in ("first_hint", "last_hint", "nights_hint"))
    monkeypatch.setattr(rr, "SERVE_UNDER_HINTS", True)
    cols = _cols(rr.descriptors()[rr.TRIPS])
    # io-kit PR 325: under sits on the paired column, naming the plain column it is drawn beneath.
    assert {k: c["hint"]["under"] for k, c in cols.items() if "under" in c.get("hint", {})} == {
        "first_hint": "first_date", "last_hint": "last_date", "nights_hint": "nights"}  # no key line (io 2026-11-25)
    assert cols["first_hint"]["hint"]["tone"] == cols["last_hint"]["hint"]["tone"] == {"inferred": "soft", "not settled": "soft"}
    assert "tone" not in cols["key"]["hint"] and "tone" not in cols["nights_hint"]["hint"]
    for k, c in cols.items():  # REC-C75: never itself, never a missing or hinted column; tones dim or soft
        h = c.get("hint", {})
        if "under" in h:
            assert h["under"] != k and h["under"] in cols and "under" not in cols[h["under"]].get("hint", {})
            assert "default_hidden" not in h
            assert set(h.get("tone", {}).values()) <= {"dim", "soft"}
    assert all(cols[k]["hint"]["default_hidden"] for k in rr.HIDDEN_WHEN_HINTED)
    assert cols["key"]["hint"] == {"default_hidden": True}  # served for the copy button, never drawn
    assert cols["nights"]["hint"] == {"align": "end", "default_width": 88}
    assert {k: c["hint"].get("default_width") for k, c in cols.items() if "default_width" in c.get("hint", {})} == {
        "first_date": 132, "last_date": 132, "nights": 88, "state": 108}  # the Travel board's grid; place takes the rest
    assert "source" not in cols  # canvas A6.1: lineage stays in the row's sources
    tones = {rr.date_hint(b, r, y) for b, r, y in [("derived", "forced", None), ("undecided", None, None)]}
    assert tones == set(rr.SOFT)  # the toned words are words the door serves


@pytest.mark.parametrize("basis,rule,by,words", [
    ("recorded", None, "flight", "from a flight"), ("recorded", None, "drive", "from a drive"),
    ("recorded", None, "correction", "from a correction"), ("derived", "from_lodging", None, "from a stay"),
    ("derived", "forced", None, "inferred"), ("derived", "a_move", None, "inferred"),
    ("undecided", "bounded", None, "not settled"), ("undecided", None, None, "not settled")])
def test_date_hints_use_the_canvas_words(basis, rule, by, words):
    assert rr.date_hint(basis, rule, by) == words


def test_engine_says_what_set_each_recorded_boundary():
    t = _trip([fl("f1", "2024-03-01", SEA, JFK), dict(drive("d1", "2024-03-05", JFK, SEA), destination="Home Town")])
    assert (t["first_by"], t["last_by"]) == ("flight", "drive")
    row = rr._trip_row(t)["values"]
    assert (row["first_hint"], row["last_hint"], row["nights_hint"]) == ("from a flight", "from a drive", "exact")
    open_ended = _trip([fl("f1", "2024-03-01", SEA, JFK)])
    assert open_ended["last_by"] is None
    v = rr._trip_row(open_ended)["values"]
    assert v["last_hint"] == "not settled" and v["nights_hint"] == "not exact"


def test_a_returned_correction_reads_from_a_correction():
    recs = [fl("f1", "2024-03-01", SEA, JFK)]
    corr = [{"correction_id": "correction-0000000000000001", "kind": "returned", "date": "2024-03-04",
             "record_ids": ["f1"], "status": "active", "created_at": "2024-03-05T00:00:00Z"}]
    t = run(recs, corrections=corr)["trips"][0]
    assert t["last_by"] == "correction" and rr._trip_row(t)["values"]["last_hint"] == "from a correction"


def test_the_meta_line_is_the_created_time_alone():
    # A14 (DOC-E97012DCAC20 v33): the session is id text; it stays served, hidden, and never joins the meta line
    c = {"correction_id": "correction-0000000000000001", "session": "SES-1", "created_at": "2026-01-02T03:04:05Z"}
    assert rr.correction_meta(c) == "2026-01-02 03:04"
    row = rr._correction_row(c)
    assert row["values"]["meta"] == "2026-01-02 03:04" and row["values"]["session"] == "SES-1"
    col = next(x for x in rr.descriptors()[rr.CORRECTIONS]["columns"] if x["id"] == "session")
    assert col["hint"]["default_hidden"]


def test_every_corrections_row_is_named_kind_word_then_date():
    # lane 2AS (3A pass 3 N27): a hidden text column "label" names the row; a kind with no date carries its made day
    col = next(c for c in rr.descriptors()[rr.CORRECTIONS]["columns"] if c["id"] == "label")
    assert col == {"id": "label", "label": "name", "type": "text", "editable": False, "hint": {"default_hidden": True}}
    base = {"correction_id": "correction-0000000000000001", "created_at": "2026-01-02T03:04:05Z", "reason": "a reason"}
    cases = [({"kind": "returned", "date": "2026-01-01", "date_iso": "2026-01-01"}, "returned · 2026-01-01"),
             ({"kind": "leave_out", "date_iso": "2026-01-02"}, "leave out · 2026-01-02"),
             ({"kind": "confirmed", "first_date": "2025-12-01", "last_date": "2025-12-04"}, "confirmed · 2026-01-02")]
    for extra, want in cases:
        row = rr._correction_row({**base, **extra})
        assert row["values"]["label"] == want and "a reason" not in want and "correction-" not in want


def test_the_label_is_outside_the_version():
    c = {"correction_id": "correction-0000000000000001", "kind": "together", "date_iso": "2026-01-02",
         "created_at": "2026-01-02T03:04:05Z"}
    one = rr._correction_row(c)
    renamed = dict(rr.KIND_LABELS, together="one trip")
    import pytest  # noqa: PLC0415
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(rr, "KIND_LABELS", renamed)
        two = rr._correction_row(c)
    assert one["values"]["label"] != two["values"]["label"] and one["version"] == two["version"]


def test_the_corrections_row_draws_date_under_kind_and_meta_under_reason(monkeypatch):
    monkeypatch.setattr(rr, "SERVE_UNDER_HINTS", False)
    off = {c["id"]: c for c in rr.descriptors()[rr.CORRECTIONS]["columns"]}
    assert not any("under" in c.get("hint", {}) for c in off.values())
    monkeypatch.setattr(rr, "SERVE_UNDER_HINTS", True)
    cols = {c["id"]: c for c in rr.descriptors()[rr.CORRECTIONS]["columns"]}
    assert {k: c["hint"]["under"] for k, c in cols.items() if "under" in c.get("hint", {})} == {"date": "kind", "meta": "reason"}
    assert cols["date"]["editable"] is True and cols["kind"]["hint"] == {"default_width": 104} and "hint" not in cols["reason"]
    assert cols["author"]["hint"]["default_hidden"]
