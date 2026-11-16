"""A-9 and A-10 (FLY-TSK-029-0D): the trip engine, on fixtures.

A-9  every record is in exactly one trip or flagged (partition property, randomised); two reads agree;
     out and back is one trip; two cities is one trip; a day out and back is none; a move is not a trip;
     a trip with no flight is a trip.
A-10 every boundary states its basis (contract.BASES, a derived one naming a contract.RULES rule); a trip
     closed on a check-out reads derived from lodging; the flight tools' six existing test files pass
     unchanged (they are not edited by this lane: see test_existing_flight_test_files_untouched).

Offline, no AWS, no io data. The io-file measurement is in test_trip_engine_io_files.py.
"""
import json
import random
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import contract as c  # noqa: E402
import trip_engine as te  # noqa: E402

TODAY = "2024-12-01"
SEA, JFK, BOS, SFO, PDX, DEN = ("apt:SEA", "city:seattle"), ("apt:JFK", "city:new-york"), ("apt:BOS", "city:boston"), \
    ("apt:SFO", "city:san-francisco"), ("apt:PDX", "city:portland"), ("apt:DEN", "city:denver")
HOME1 = {"residence_uid": "R1", "first_night": "2020-01-01", "last_night": None, "city": "Seattle",
         "places": [{"place_id": "apt:SEA", "area_id": "city:seattle"}]}


def fl(fid, date, o, d, **kw):
    return {"flight_id": fid, "date_iso": date, "origin": o[0][4:], "dest": d[0][4:], "origin_place_id": o[0],
            "origin_area_id": o[1], "dest_place_id": d[0], "dest_area_id": d[1], "status": "active", **kw}


def lodge(rid, first, last, place, **kw):
    return {"kind": "lodging", "record_id": rid, "first_night": first, "last_night": last, "status": "active",
            "place_id": place[0], "area_id": place[1], **kw}


def car(rid, pick, drop, place):
    return {"kind": "car_rental", "record_id": rid, "pickup_date": pick, "dropoff_date": drop, "status": "active",
            "pickup_place_id": place[0], "pickup_area_id": place[1]}


def drive(rid, date, o, d, seq=0):
    return {"kind": "drive", "record_id": rid, "date": date, "sequence": seq, "status": "active",
            "from_place_id": o[0], "from_area_id": o[1], "to_place_id": d[0], "to_area_id": d[1]}


def run(records, periods=(HOME1,), today=TODAY, **kw):
    return te.compute_trips(records, list(periods), today=today, **kw)


def partition_ok(res, records):
    ids = [r.get("flight_id") or r.get("record_id") for r in records]
    in_trips = [i for t in res["trips"] for i in t["record_ids"]]
    out = [x["record_id"] for x in res["excluded"]]
    assert len(in_trips) == len(set(in_trips)), "a record is in two trips"
    assert len(out) == len(set(out)), "a record is excluded twice"
    assert not set(in_trips) & set(out), "a record is both in a trip and excluded"
    assert sorted(set(in_trips) | set(out)) == sorted(set(ids)), "a record is in no trip and carries no disposition"
    assert all(x["disposition"] in te.DISPOSITIONS and x["detail"] for x in res["excluded"])


def boundaries_ok(res):
    rules = {n for n, r in c.RULES.items() if r["derives_boundary"]}
    for t in res["trips"]:
        for side in ("first", "last"):
            basis, rule = t[f"{side}_basis"], t[f"{side}_rule"]
            assert basis in c.BASES
            if basis == "derived":
                assert rule in rules, f"a derived boundary must name a deriving rule, got {rule}"
            elif basis == "recorded":
                assert rule is None
            else:
                assert rule in (None, "bounded")
        assert set(t["flags"]) <= set(c.FLAGS) and set(t["signals"]) <= set(c.SIGNALS)
        assert t["state"] in c.TRIP_STATES
        assert set(c.TRIP_FIELDS) | set(c.GET_TRIP_EXTRA_FIELDS) <= set(t)


# ------------------------------------------------------------------ A-9 shape cases
def test_out_and_back_is_one_trip():
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-05", JFK, SEA)]
    res = run(recs)
    assert len(res["trips"]) == 1 and res["excluded"] == []
    t = res["trips"][0]
    assert (t["first_date"], t["first_basis"], t["last_date"], t["last_basis"], t["nights"]) == \
           ("2024-03-01", "recorded", "2024-03-05", "recorded", 4)
    assert t["flags"] == [] and t["key"] == "f1" and t["state"] == "past"
    partition_ok(res, recs)
    boundaries_ok(res)


def test_two_cities_is_one_trip_and_connections_are_not_visits():
    recs = [fl("f1", "2024-03-01", SEA, DEN), fl("f2", "2024-03-01", DEN, JFK),  # DEN is a connection
            fl("f3", "2024-03-03", JFK, BOS), fl("f4", "2024-03-06", BOS, SEA)]
    res = run(recs)
    assert len(res["trips"]) == 1
    assert res["trips"][0]["places_visited"] == ["city:new-york", "city:boston"]
    assert res["trips"][0]["last_basis"] == "recorded"
    partition_ok(res, recs)


def test_same_day_connection_order_follows_the_chain_not_the_id():
    recs = [fl("z1", "2024-03-01", SEA, DEN), fl("a1", "2024-03-01", DEN, JFK), fl("m1", "2024-03-04", JFK, SEA)]
    t = run(recs)["trips"][0]
    assert [l["record_id"] for l in t["legs"]] == ["z1", "a1", "m1"] and t["first_basis"] == "recorded"


def test_a_day_out_and_back_is_none():
    recs = [fl("f1", "2024-03-01", SEA, SFO), fl("f2", "2024-03-01", SFO, SEA)]
    res = run(recs)
    assert res["trips"] == []
    assert {x["disposition"] for x in res["excluded"]} == {"day_out_and_back"}
    partition_ok(res, recs)


def test_a_move_is_not_a_trip():
    p2 = {"residence_uid": "R2", "first_night": "2024-06-01", "last_night": None, "city": "San Francisco",
          "places": [{"place_id": "apt:SFO", "area_id": "city:san-francisco"}]}
    p1 = dict(HOME1, last_night="2024-05-31")
    recs = [fl("mv", "2024-06-01", SEA, SFO)]
    res = run(recs, periods=(p1, p2))
    assert res["trips"] == [] and res["excluded"][0]["disposition"] == "a_move"
    partition_ok(res, recs)


def test_arriving_at_a_new_residence_closes_a_trip_by_rule_a_move():
    p2 = {"residence_uid": "R2", "first_night": "2024-06-01", "last_night": None, "city": "San Francisco",
          "places": [{"place_id": "apt:SFO", "area_id": "city:san-francisco"}]}
    p1 = dict(HOME1, last_night="2024-05-31")
    recs = [fl("f1", "2024-05-20", SEA, JFK), fl("f2", "2024-06-02", JFK, SFO)]
    res = run(recs, periods=(p1, p2))
    t = res["trips"][0]
    assert (t["last_date"], t["last_basis"], t["last_rule"]) == ("2024-06-02", "derived", "a_move")
    # before the first night it is a trip, not a move
    early = run([fl("f1", "2024-05-20", SEA, JFK), fl("f2", "2024-05-25", JFK, SFO)], periods=(p1, p2))
    assert early["trips"][0]["last_basis"] == "undecided"


def test_a_trip_with_no_flight_is_a_trip():
    recs = [lodge("lodging-1", "2024-07-01", "2024-07-03", PDX)]
    res = run(recs)
    t = res["trips"][0]
    assert t["legs"] == [] and t["nights"] == 3
    assert (t["first_date"], t["first_basis"], t["first_rule"]) == ("2024-07-01", "derived", "from_lodging")
    assert (t["last_date"], t["last_basis"], t["last_rule"]) == ("2024-07-04", "derived", "from_lodging")
    assert t["flags"] == [] and t["key"] == "lodging-1"
    # a drive away and back, with no flight and no lodging
    recs = [drive("drive-1", "2024-07-10", SEA, PDX), drive("drive-2", "2024-07-12", PDX, SEA)]
    res = run(recs)
    assert len(res["trips"]) == 1 and res["trips"][0]["legs"][0]["kind"] == "drive" and res["trips"][0]["nights"] == 2
    partition_ok(res, recs)


def test_two_back_to_back_stays_in_two_cities_is_one_trip_with_no_flight():
    recs = [lodge("lodging-a", "2024-07-01", "2024-07-03", PDX), lodge("lodging-b", "2024-07-04", "2024-07-06", SFO)]
    res = run(recs)
    assert len(res["trips"]) == 1 and res["trips"][0]["places_visited"] == ["city:portland", "city:san-francisco"]


# ------------------------------------------------------------------ A-10
def test_a_trip_closed_on_a_check_out_reads_derived_from_lodging():
    recs = [fl("f1", "2024-03-01", SEA, JFK), lodge("lodging-ny", "2024-03-01", "2024-03-04", JFK)]
    res = run(recs)
    t = res["trips"][0]
    assert (t["last_date"], t["last_basis"], t["last_rule"]) == ("2024-03-05", "derived", "from_lodging")
    assert t["first_basis"] == "recorded" and t["flags"] == [] and t["nights"] == 4
    assert t["stays"][0]["record_id"] == "lodging-ny"
    partition_ok(res, recs)
    boundaries_ok(res)


def test_a_trip_opened_on_a_check_in_reads_derived_from_lodging():
    recs = [lodge("lodging-ny", "2024-03-01", "2024-03-04", JFK), fl("f2", "2024-03-05", JFK, SEA)]
    t = run(recs)["trips"][0]
    assert (t["first_date"], t["first_basis"], t["first_rule"]) == ("2024-03-01", "derived", "from_lodging")
    assert t["last_basis"] == "recorded"


def test_forced_and_bounded_ends():
    # the only possible return date is the day the next trip leaves home
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-10", JFK, BOS), fl("f3", "2024-03-10", SEA, DEN),
            fl("f4", "2024-03-12", DEN, SEA)]
    t = run(recs)["trips"][0]
    assert (t["last_date"], t["last_basis"], t["last_rule"], t["flags"]) == ("2024-03-10", "derived", "forced", [])
    # otherwise only a bound: back by the next departure from home. Undecided, and flagged.
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-03", JFK, BOS), fl("f3", "2024-03-20", SEA, DEN),
            fl("f4", "2024-03-22", DEN, SEA)]
    t = run(recs)["trips"][0]
    assert (t["last_date"], t["last_basis"], t["last_rule"], t["flags"]) == ("2024-03-20", "undecided", "bounded", ["return not recorded"])
    assert t["nights_exact"] is False and t["last_evidence_date"] == "2024-03-03"


def test_flags():
    t = run([fl("f1", "2024-03-05", JFK, SEA)])["trips"][0]
    assert t["flags"] == ["start not recorded"] and t["first_basis"] == "undecided"
    t = run([fl("f1", "2024-03-01", SEA, JFK)])["trips"][0]
    assert t["flags"] == ["return not recorded"] and t["state"] == "past"
    t = run([fl("f1", "2025-03-01", SEA, JFK)])["trips"][0]
    assert t["flags"] == ["return not planned yet"] and t["state"] == "planned"
    t = run([fl("f1", "2019-03-01", SEA, JFK), fl("f2", "2019-03-04", JFK, SEA)])["trips"][0]
    assert t["flags"] == ["home not recorded for these dates"]
    # a bound on the start: left on or after the previous arrival
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-03", JFK, SEA), fl("f3", "2024-03-10", BOS, SEA)]
    t = run(recs)["trips"][1]
    assert (t["first_date"], t["first_basis"], t["first_rule"]) == ("2024-03-03", "undecided", "bounded")
    # a place near home but not listed
    h = dict(HOME1, places=[{"place_id": "apt:SEA", "area_id": "city:seattle", "lat": 47.6, "lon": -122.33}])
    near = lodge("lodging-b", "2024-07-01", "2024-07-02", ("addr:bellevue-1", "city:bellevue"), lat=47.61, lon=-122.2)
    assert run([near], periods=(h,))["trips"][0]["flags"] == ["is this place home"]


def test_no_home_at_all_refuses():
    with pytest.raises(te.NoHomeRecorded):
        run([fl("f1", "2024-03-01", SEA, JFK)], periods=())


def test_signals():
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-02", SEA, JFK), fl("f3", "2024-03-05", JFK, SEA)]
    assert "likely duplicate" in run(recs)["trips"][0]["signals"]
    recs = []
    for i in range(10):
        recs += [fl(f"a{i}", f"2023-{i + 1:02d}-01", SEA, JFK), fl(f"b{i}", f"2023-{i + 1:02d}-04", JFK, SEA)]
    recs += [fl("long1", "2024-01-01", SEA, JFK), fl("long2", "2024-03-30", JFK, SEA)]
    sig = {t["key"]: t["signals"] for t in run(recs)["trips"]}
    assert sig["long1"] == ["unusually long"] and all(v == [] for k, v in sig.items() if k != "long1")


def test_corrections():
    recs = [fl("f1", "2024-03-01", SEA, JFK)]
    corr = [{"kind": "returned", "date": "2024-03-06", "record_ids": ["f1"], "author": "io", "status": "active"}]
    t = run(recs, corrections=corr)["trips"][0]
    assert (t["last_date"], t["last_basis"], t["flags"]) == ("2024-03-06", "recorded", [])
    assert t["corrections"][0]["author"] == "io"
    t = run(recs, corrections=[dict(corr[0], status="retired")])["trips"][0]
    assert t["last_basis"] == "undecided"
    res = run(recs + [fl("f2", "2024-03-02", SEA, BOS)], corrections=[{"kind": "leave_out", "record_ids": ["f2"]}])
    assert [x["disposition"] for x in res["excluded"]] == ["left_out"]
    a, b = [fl("f1", "2024-03-01", SEA, JFK), fl("f2", "2024-03-02", JFK, SEA)], [fl("f3", "2024-03-03", SEA, BOS), fl("f4", "2024-03-05", BOS, SEA)]
    assert len(run(a + b)["trips"]) == 2
    joined = run(a + b, corrections=[{"kind": "together", "record_ids": ["f1", "f3"]}])
    assert len(joined["trips"]) == 1 and joined["unapplied_corrections"] == []


def test_listing_order_and_range():
    recs = [fl("p1", "2024-01-01", SEA, JFK), fl("p2", "2024-01-03", JFK, SEA), fl("p3", "2024-06-01", SEA, JFK),
            fl("p4", "2024-06-03", JFK, SEA), fl("n1", "2024-12-01", SEA, JFK), fl("u1", "2025-02-01", SEA, JFK),
            fl("u2", "2025-02-04", JFK, SEA)]
    res = run(recs)
    assert [t["state"] for t in te.order_trips_for_listing(res["trips"])] == ["under way", "upcoming", "past", "past"]
    assert [t["key"] for t in te.order_trips_for_listing(res["trips"])][2:] == ["p3", "p1"]
    assert [t["key"] for t in te.trips_in_range(res["trips"], "2024-06-02", "2024-06-30")] == ["p3"]


# ------------------------------------------------------------------ A-9 partition and determinism, randomised
def _random_records(rng, n):
    places = [SEA, JFK, BOS, SFO, PDX, DEN]
    recs = []
    for i in range(n):
        day = f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
        k = rng.choice(["flight", "flight", "flight", "lodging", "car_rental", "drive", "bad", "dup", "cancelled"])
        o, d = rng.sample(places, 2)
        if k == "flight":
            recs.append(fl(f"f{i}", day, o, d))
        elif k == "drive":
            recs.append(drive(f"drive-{i}", day, o, d, seq=rng.randint(0, 2)))
        elif k == "lodging":
            first = rng.randint(1, 20)
            recs.append(lodge(f"lodging-{i}", f"2024-{rng.randint(1, 12):02d}-{first:02d}", f"2024-{12:02d}-28", o) if rng.random() < .1
                        else lodge(f"lodging-{i}", day, day, o))
        elif k == "car_rental":
            recs.append(car(f"car_rental-{i}", day, day, o))
        elif k == "bad":
            recs.append({"kind": "lodging", "record_id": f"lodging-bad{i}", "status": "active"})
        elif k == "dup" and recs:
            recs.append(dict(rng.choice(recs)))
        else:
            recs.append(dict(fl(f"f{i}", day, o, d), status="cancelled"))
    return recs


@pytest.mark.parametrize("seed", range(40))
def test_partition_property_and_two_reads_agree(seed):
    rng = random.Random(seed)
    recs = _random_records(rng, rng.randint(1, 120))
    res = run(recs)
    partition_ok(res, recs)
    boundaries_ok(res)
    again = run(recs)
    shuffled = recs[:]
    rng.shuffle(shuffled)
    assert json.dumps(res, sort_keys=True) == json.dumps(again, sort_keys=True)
    assert json.dumps(res, sort_keys=True) == json.dumps(run(shuffled), sort_keys=True), "input order changed the answer"


def test_unreadable_and_duplicate_and_cancelled_are_stated():
    recs = [fl("f1", "2024-03-01", SEA, JFK), fl("f1", "2024-03-01", SEA, JFK), dict(fl("f9", "2024-03-02", SEA, JFK), status="cancelled"),
            {"kind": "lodging", "record_id": "lodging-x"}, {"kind": "mystery", "record_id": "mystery-1"}]
    res = run(recs)
    assert sorted(x["disposition"] for x in res["excluded"]) == ["cancelled", "unreadable", "unreadable"]
    assert res["duplicate_inputs"] == ["f1"] and len(res["trips"]) == 1


def test_no_wall_clock_or_storage_imports():
    src = (D / "trip_engine.py").read_text()
    for banned in ("boto3", "import os", "datetime.now", "date.today", "time.time", "random", "requests"):
        assert banned not in src


def test_engine_is_not_a_handler_module():
    assert not hasattr(te, "HANDLERS") and "trip_engine" not in c.LANE_MODULES.values()


# ------------------------------------------------------------------ A-10: six existing flight test files unchanged
SIX = ["test_search_flights.py", "test_e2e_lambda_handler.py", "test_dual_issuer.py", "test_mcp_edge_adoption.py",
       "test_dry_run.py", "test_release_ops.py"]


def test_existing_flight_test_files_untouched():
    """The lane's diff against its merge base with origin/main touches none of the pre-existing test files."""
    try:
        base = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
        diff = subprocess.run(["git", "diff", "--name-only", base, "--", "tests"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("no git history available")
    changed = {Path(p).name for p in diff}
    assert not changed & set(SIX), changed & set(SIX)
