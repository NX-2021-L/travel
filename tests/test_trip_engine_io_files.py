"""A-9, on io's flight log and TripIt export as FILES (BRD DOC-14994CDDA217 section 7.5).

The files are fetched by id (`elr doc_get DOC-90FFE928B551`, `elr doc_get DOC-4D7DA6D20329`) into
~/.enceladus/elr/cache and are NEVER copied into this repository. This test reads them at test time and skips
cleanly when they are absent, so CI stays green and a local run does the measurement.

STAND-IN MEASUREMENT. No residence period is recorded for io, so home here is inferred from the data (the busiest
origin metro of each year, smoothed against its neighbours) with a hand-made metro table. BRD section 0.1: none of
the figures this prints is an acceptance threshold or a result; acceptance is lane 4D's repeat on io's recorded
residence periods. The assertions are structural (the partition holds, the counts reconcile), never about a figure.
"""
import collections
import csv
import io
import json
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"))
import trip_engine as te  # noqa: E402

CACHE = Path(os.environ.get("ELR_CACHE", Path.home() / ".enceladus" / "elr" / "cache"))
FLIGHT_LOG, TRIPIT = CACHE / "DOC-90FFE928B551.md", CACHE / "DOC-4D7DA6D20329.md"
TODAY = "2026-11-12"  # the log's snapshot date; fixed, not the clock

METRO = {}
for slug, codes in {"new-york": "JFK LGA EWR", "san-francisco": "SFO OAK SJC", "los-angeles": "LAX BUR SNA LGB ONT",
                    "washington": "DCA IAD BWI", "chicago": "ORD MDW", "dallas": "DFW DAL", "houston": "IAH HOU",
                    "miami": "MIA FLL PBI", "seattle": "SEA", "portland": "PDX", "boston": "BOS", "london": "LHR LGW"}.items():
    for code in codes.split():
        METRO[code] = f"city:{slug}"


def _area(code):
    return METRO.get(code, f"city:{code.lower()}")


def _flight(fid, date, o, d, seq=0):
    return {"flight_id": fid, "date_iso": date, "origin": o, "dest": d, "origin_place_id": f"apt:{o}",
            "origin_area_id": _area(o), "dest_place_id": f"apt:{d}", "dest_area_id": _area(d), "status": "active", "sequence": seq}


def _stand_in_home(flights):
    """Busiest origin metro per year, a year that disagrees with equal neighbours taken from them; eras as periods."""
    by = collections.defaultdict(collections.Counter)
    for f in flights:
        by[int(f["date_iso"][:4])][f["origin_area_id"]] += 1
    years = sorted(by)
    top = {y: sorted(by[y].items(), key=lambda kv: (-kv[1], kv[0]))[0][0] for y in years}
    for y in years[1:-1]:
        if top[y - 1] == top[y + 1] != top[y] and y - 1 in top and y + 1 in top:
            top[y] = top[y - 1]
    periods, areas = [], {}
    for y in years:
        if periods and areas[periods[-1]["residence_uid"]] == top[y]:
            continue
        uid = f"era-{y}"
        areas[uid] = top[y]
        periods.append({"residence_uid": uid, "first_night": f"{y}-01-01", "last_night": None, "city": top[y],
                        "places": [{"place_id": None, "area_id": top[y]}]})
    for a, b in zip(periods, periods[1:]):
        a["last_night"] = (te._d(b["first_night"]) - te._ONE).isoformat()
    return periods


def _report(name, records, res, periods):
    trips, ex = res["trips"], res["excluded"]
    in_trips = sum(len(t["record_ids"]) for t in trips)
    flagged = collections.Counter(f for t in trips for f in t["flags"])
    print(f"\n[STAND-IN MEASUREMENT, home inferred, not an acceptance figure] {name}")
    print(f"  records {len(records)}: in trips {in_trips}, excluded {len(ex)} {dict(collections.Counter(x['disposition'] for x in ex))}")
    print(f"  trips {len(trips)}: with a flag {sum(1 for t in trips if t['flags'])}, no flags {sum(1 for t in trips if not t['flags'])}; flags {dict(flagged)}")
    print(f"  signals {dict(collections.Counter(s for t in trips for s in t['signals']))}; "
          f"boundary bases first {dict(collections.Counter(t['first_basis'] for t in trips))} last {dict(collections.Counter(t['last_basis'] for t in trips))}")
    print(f"  derived by rule {dict(collections.Counter(r for t in trips for r in (t['first_rule'], t['last_rule']) if r))}; "
          f"eras {len(periods)}; trips with no flight {sum(1 for t in trips if not any(l['kind'] == 'flight' for l in t['legs']))}")
    ids = [r.get("flight_id") or r.get("record_id") for r in records]
    all_out = [i for t in trips for i in t["record_ids"]] + [x["record_id"] for x in ex]
    assert sorted(all_out) == sorted(set(ids))  # the partition, on io's records
    again = te.compute_trips(list(reversed(records)), periods, today=TODAY)
    assert json.dumps(again, sort_keys=True) == json.dumps(res, sort_keys=True)  # two reads agree, input order aside


@pytest.mark.skipif(not FLIGHT_LOG.exists(), reason="io's flight log (DOC-90FFE928B551) is fetched by id and absent here")
def test_io_flight_log_as_a_file():
    block = re.search(r"```csv\n(.*?)```", FLIGHT_LOG.read_text(), re.S).group(1)
    rows = list(csv.DictReader(io.StringIO(block)))
    flights = [_flight(f"log-{i:04d}", r["date"], r["origin"], r["dest"]) for i, r in enumerate(rows)]
    assert len(flights) > 500
    periods = _stand_in_home(flights)
    res = te.compute_trips(flights, periods, today=TODAY)
    _report("flight log", flights, res, periods)


def _tripit_records():
    text = TRIPIT.read_text()
    recs, n = [], 0
    for blk in re.findall(r"```json\n(.*?)\n```", text, re.S):
        for tid, name, loc, dates, flights, lodging, cars, _o in json.loads(blk)["trips"]:
            area = "city:" + re.sub(r"[^a-z0-9]+", "-", loc.split(",")[0].strip().lower()).strip("-")  # stand-in: the trip's own city label
            for date, _t, _tz, o, d, num, *_ in flights:
                n += 1
                recs.append(_flight(f"tp-{n:04d}", date, o, d))
            for kind, evs, open_w, close_w in (("lodging", lodging, "check in", "check out"), ("car_rental", cars, "pick", "drop")):
                opened = {}
                for date, nm, action, _addr in evs:
                    a = action.lower()
                    if a.startswith(open_w):
                        opened[nm] = date
                    elif a.startswith(close_w) and nm in opened:
                        n += 1
                        first = opened.pop(nm)
                        if kind == "lodging":
                            last = (te._d(date) - te._ONE).isoformat()
                            recs.append({"kind": "lodging", "record_id": f"lodging-{n:04d}", "first_night": first,
                                         "last_night": max(first, last), "place_id": None, "area_id": area, "status": "active"})
                        else:
                            recs.append({"kind": "car_rental", "record_id": f"car_rental-{n:04d}", "pickup_date": first, "dropoff_date": date,
                                         "pickup_place_id": None, "pickup_area_id": area, "status": "active"})
    return recs


@pytest.mark.skipif(not TRIPIT.exists(), reason="io's TripIt export (DOC-4D7DA6D20329) is fetched by id and absent here")
def test_io_tripit_export_as_a_file():
    recs = _tripit_records()
    flights = [r for r in recs if "flight_id" in r]
    assert len(flights) > 150
    periods = _stand_in_home(flights)
    for p in periods:  # lodging and rentals carry a metro only; give the home metro to the place test by area
        assert p["places"][0]["area_id"]
    res = te.compute_trips(recs, periods, today=TODAY)
    _report("tripit export (flights, lodging, rentals)", recs, res, periods)
    res_f = te.compute_trips(flights, periods, today=TODAY)
    _report("tripit export (flights only)", flights, res_f, periods)
