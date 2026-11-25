"""FLY-TSK-093 (DVP-PLN-021 lane 2Z): trips rows' source refs carry the chip text the Travel board draws.

"flight · 2026-11-06", "stay · 3 nights" (canvas A7.7): one label per record the trip came from, from the trip's own legs, stays
and rentals. SERVE_SOURCE_LABELS is on since think went live on the W2b kit (RowSourceRef.label, kit lane 2Y).
Synthetic values only.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
sys.path.insert(0, str(ROOT / "tests"))
import records_routes as rr  # noqa: E402
from test_trip_engine import JFK, SEA, drive, fl, lodge, car, run  # noqa: E402

TRIP = {"key": "aaaaaaaaaaaa", "record_ids": ["aaaaaaaaaaaa", "lodging-0000000000000001", "car_rental-0000000000000002",
                                              "drive-0000000000000003", "lodging-0000000000000004", "orphan-1"],
        "legs": [{"record_id": "aaaaaaaaaaaa", "kind": "flight", "date": "2026-11-06"},
                 {"record_id": "drive-0000000000000003", "kind": "drive", "date": "2026-11-09"}],
        "stays": [{"record_id": "lodging-0000000000000001", "first_night": "2026-11-06", "checkout": "2026-11-09"},
                  {"record_id": "lodging-0000000000000004", "first_night": "2026-11-08", "checkout": "2026-11-09"}],
        "rentals": [{"record_id": "car_rental-0000000000000002", "pickup_date": "2026-11-06", "dropoff_date": "2026-11-08"}]}


def test_served_now_think_is_on_the_w2b_kit(monkeypatch):
    assert rr.SERVE_SOURCE_LABELS  # think is live on @io-kit/records 0.7.0 (intelligence 958)
    monkeypatch.setattr(rr, "SERVE_SOURCE_LABELS", False)
    row = rr._trip_row(TRIP)
    assert all(set(s) == {"recordset", "key"} for s in row["sources"])


def test_each_source_reads_as_the_board_draws_it(monkeypatch):
    monkeypatch.setattr(rr, "SERVE_SOURCE_LABELS", True)
    row = rr._trip_row(TRIP)
    labels = {s["key"]: s.get("label") for s in row["sources"]}
    assert labels == {"aaaaaaaaaaaa": "flight · 2026-11-06", "lodging-0000000000000001": "stay · 3 nights",
                      "car_rental-0000000000000002": "car rental · 2 days", "drive-0000000000000003": "drive · 2026-11-09",
                      "lodging-0000000000000004": "stay · 1 night", "orphan-1": None}
    assert row["source"] == row["sources"][0] and row["sources"][0]["recordset"] == rr.SOURCE_SET


def test_every_record_of_an_engine_trip_is_labelled():
    t = run([fl("f1", "2024-03-01", SEA, JFK), lodge("l1", "2024-03-01", "2024-03-02", JFK),
             car("c1", "2024-03-01", "2024-03-03", JFK), dict(drive("d1", "2024-03-03", JFK, JFK)),
             fl("f2", "2024-03-04", JFK, SEA)])["trips"][0]
    labels = rr.source_labels(t)
    assert set(labels) == set(t["record_ids"])
    assert labels["f1"] == "flight · 2024-03-01" and labels["l1"] == "stay · 2 nights"
