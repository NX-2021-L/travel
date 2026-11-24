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
    assert shown == ["correction_id", "kind", "date", "reason", "created_at"]
    assert {c["id"] for c in cols} >= {"author", "session", "record_ids", "method", "supersedes"}


def test_under_hints_are_off_until_the_w2b_kit_is_live(monkeypatch):
    off = _cols(rr.descriptors()[rr.TRIPS])
    assert not any("under" in c.get("hint", {}) or "tone" in c.get("hint", {}) for c in off.values())
    assert not any(off[k].get("hint", {}).get("default_hidden") for k in ("key", *rr.HIDDEN_WHEN_HINTED))
    assert all(off[k]["hint"]["default_hidden"] for k in ("first_hint", "last_hint", "nights_hint"))
    monkeypatch.setattr(rr, "SERVE_UNDER_HINTS", True)
    cols = _cols(rr.descriptors()[rr.TRIPS])
    # io-kit PR 325: under sits on the paired column, naming the plain column it is drawn beneath.
    assert {k: c["hint"]["under"] for k, c in cols.items() if "under" in c.get("hint", {})} == {
        "key": "place", "first_hint": "first_date", "last_hint": "last_date", "nights_hint": "nights"}
    assert cols["first_hint"]["hint"]["tone"] == cols["last_hint"]["hint"]["tone"] == {"inferred": "soft", "not settled": "soft"}
    assert "tone" not in cols["key"]["hint"] and "tone" not in cols["nights_hint"]["hint"]
    for k, c in cols.items():  # REC-C75: never itself, never a missing or hinted column; tones dim or soft
        h = c.get("hint", {})
        if "under" in h:
            assert h["under"] != k and h["under"] in cols and "under" not in cols[h["under"]].get("hint", {})
            assert "default_hidden" not in h
            assert set(h.get("tone", {}).values()) <= {"dim", "soft"}
    assert all(cols[k]["hint"]["default_hidden"] for k in rr.HIDDEN_WHEN_HINTED)
    assert cols["nights"]["hint"] == {"align": "end"}
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
