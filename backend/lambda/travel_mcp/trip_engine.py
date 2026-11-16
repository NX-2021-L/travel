"""The trip engine (FLY-TSK-029-0D, BRD DOC-14994CDDA217 section 6.5, lane 2D).

Pure computation: no storage, no network, no wall clock. Records and residence periods in,
trips out. Lanes 3A (list_trips, get_trip) and 3B import this module; it exposes no HANDLERS and
is not in contract.LANE_MODULES.

ENTRY POINT
    compute_trips(records, home_periods, *, today, home_places=(), corrections=(),
                  near_home_km=50.0) -> dict

    records        flight rows (carry `flight_id`), and lodging / car_rental / drive records
                   (carry `kind` and `record_id`), as dicts. Any status; cancelled ones are
                   excluded with a stated disposition.
    home_periods   residence periods as finance returns them: residence_uid, first_night,
                   last_night (None = open), optional city/state/country/tenure/evidence. An
                   optional `places` list ({place_id, area_id, lat, lon, radius_km}) is accepted.
    home_places    home_places records ({residence_uid, valid_from, valid_to, places, status}).
    corrections    correction records (kind returned|left|together|leave_out, date, record_ids,
                   status, author, question_id).
    today          ISO date or date; required, because the engine never reads a clock.
    Raises NoHomeRecorded when home_periods is empty (the read's NO_HOME_RECORDED refusal).

    Returns {"trips": [...], "excluded": [...], "unapplied_corrections": [...], "duplicate_inputs": [...]}, a PARTITION:
    every input record id is in exactly one trip's `record_ids` or in exactly one `excluded`
    entry {record_id, kind, disposition, detail}; an id supplied twice is one record (listed in duplicate_inputs). Dispositions are DISPOSITIONS. Trips are in
    chronological order (first_date, key); use order_trips_for_listing for the BRD list order.

Each trip carries the contract.TRIP_FIELDS plus first_rule / last_rule (contract.GET_TRIP_EXTRA_FIELDS),
record_ids, nights_exact, and first_evidence_date / last_evidence_date. A boundary basis is one of
contract.BASES: `recorded`; `derived` with the name of the rule in first_rule / last_rule (from_lodging,
forced, a_move); or `undecided`, which still shows a bound when one exists (rule `bounded`).
A boundary set by an active correction reads `recorded`, and the correction is listed in `corrections`.

Determinism: inputs are sorted by explicit keys, sets are never iterated unsorted.
"""
import datetime as _dt
import math

import contract

DISPOSITIONS = contract.DISPOSITIONS  # the vocabulary lives in the contract
KIND_ORDER = {"flight": 0, "drive": 1, "lodging": 2, "car_rental": 3}
STATE_ORDER = {"under way": 0, "upcoming": 1, "planned": 2, "past": 3}
_ONE = _dt.timedelta(days=1)


class NoHomeRecorded(Exception):
    """No residence period at all: the read refuses with NO_HOME_RECORDED."""


def _d(v):
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    return _dt.date.fromisoformat(str(v)[:10])


def _iso(d):
    return d.isoformat() if d else None


# --------------------------------------------------------------------------- normalise
def _end(rec, pid, aid, lat=None, lon=None):
    return {"place_id": rec.get(pid) or None, "area_id": rec.get(aid) or None,
            "lat": rec.get(lat) if lat else None, "lon": rec.get(lon) if lon else None}


def _kind_of(rec):
    if rec.get("kind"):
        return rec["kind"]
    if rec.get("flight_id"):
        return "flight"
    return str(rec.get("record_id", "")).split("-")[0]


def _rid(rec):
    rid = rec.get("flight_id") or rec.get("record_id")
    if not rid:
        raise ValueError("record without flight_id or record_id")
    return rid


def _normalise(rec):
    """-> event dict, or None when the record cannot be read."""
    kind, rid = _kind_of(rec), _rid(rec)
    ev = {"id": rid, "kind": kind}
    try:
        if kind == "flight":
            o, t = rec.get("origin"), rec.get("dest")
            ev.update(date=_d(rec.get("date_iso")), seq=rec.get("sequence") or 0, time=str(rec.get("depart_time") or ""),
                      frm=_end(rec, "origin_place_id", "origin_area_id"), to=_end(rec, "dest_place_id", "dest_area_id"))
            # DOC-76D85C855CBE s5: a flight's place_id is `apt:` plus the existing origin / dest
            ev["frm"]["place_id"] = ev["frm"]["place_id"] or (f"apt:{o}" if o else None)
            ev["to"]["place_id"] = ev["to"]["place_id"] or (f"apt:{t}" if t else None)
            ok = ev["date"] and ev["frm"]["place_id"] and ev["to"]["place_id"]
        elif kind == "drive":
            ev.update(date=_d(rec.get("date")), seq=rec.get("sequence") or 0, time="",
                      frm=_end(rec, "from_place_id", "from_area_id", "from_lat", "from_lon"),
                      to=_end(rec, "to_place_id", "to_area_id", "to_lat", "to_lon"))
            ok = ev["date"] is not None
        elif kind == "lodging":
            first = _d(rec.get("first_night"))
            last = _d(rec.get("last_night")) or first
            ev.update(date=first, start=first, checkout=(last + _ONE) if last else None,
                      place=_end(rec, "place_id", "area_id", "lat", "lon"))
            ok = first is not None and last >= first
        elif kind == "car_rental":
            pick = _d(rec.get("pickup_date"))
            drop = _d(rec.get("dropoff_date")) or pick
            ev.update(date=pick, start=pick, checkout=drop, place=_end(rec, "pickup_place_id", "pickup_area_id"))
            ok = pick is not None and drop >= pick
        else:
            ok = False
    except ValueError:
        ok = False
    return ev if ok else None


# --------------------------------------------------------------------------- home
class _Home:
    def __init__(self, periods, home_places):
        ps = [{"uid": p.get("residence_uid"), "first": _d(p.get("first_night")), "last": _d(p.get("last_night")),
               "src": p, "places": list(p.get("places") or [])} for p in periods]
        ps = [p for p in ps if p["first"]]
        ps.sort(key=lambda p: (p["first"], str(p["uid"])))
        if not ps:
            raise NoHomeRecorded("no residence period recorded")
        for hp in sorted((h for h in home_places if h.get("status", "active") != "cancelled"),
                         key=lambda h: (str(h.get("residence_uid")), str(h.get("valid_from")))):
            for p in ps:
                if p["uid"] == hp.get("residence_uid"):
                    p["places"].extend(hp.get("places") or [])
        self.periods = ps

    def on(self, d):
        """-> (periods covering d, covered). With none covering, the nearest period stands in and covered is False."""
        c = [p for p in self.periods if p["first"] <= d and (p["last"] is None or d <= p["last"])]
        if c:
            return c, True

        def gap(p):
            return (p["first"] - d).days if d < p["first"] else (d - p["last"]).days
        return [sorted(self.periods, key=lambda p: (gap(p), p["first"], str(p["uid"])))[0]], False


def _hav(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def _place_is(end, hp):
    """Same node by place_id, same metro by area_id, or within the home place's radius. Never string parsing."""
    if end.get("place_id") and end["place_id"] == hp.get("place_id"):
        return True
    if end.get("area_id") and end["area_id"] == hp.get("area_id"):
        return True
    r = hp.get("radius_km")
    if r and None not in (end.get("lat"), end.get("lon"), hp.get("lat"), hp.get("lon")):
        return _hav((end["lat"], end["lon"]), (hp["lat"], hp["lon"])) <= r
    return False


def _home_uid(end, periods):
    for p in periods:
        if any(_place_is(end, hp) for hp in p["places"]):
            return p["uid"]
    return None


# --------------------------------------------------------------------------- stretches
def _stretch(m, start_rec, lo, uid):
    return {"movs": [m], "start_rec": start_rec, "end_rec": False, "lo": lo, "hi": None, "uid": uid,
            "end_rule": None, "lodging": [], "rentals": []}


def _order_day(movs):
    """Same-day movements follow the chain of places (origin = previous destination); ties by seq, time, id."""
    rest, out = sorted(movs, key=lambda m: (m["seq"], m["time"], m["id"])), []
    while rest:
        dests = [m["to"]["place_id"] for m in rest]
        cur = next((m for m in rest if m["frm"]["place_id"] not in dests), rest[0])
        while cur:
            rest.remove(cur)
            out.append(cur)
            cur = next((m for m in rest if m["frm"]["place_id"] and m["frm"]["place_id"] == cur["to"]["place_id"]), None)
    return out


def _cut(movs, home, excluded):
    """Rules home_to_home and a_move: one line of movements -> stretches away from home."""
    by_day = {}
    for m in movs:
        by_day.setdefault(m["date"], []).append(m)
    line = [m for d in sorted(by_day) for m in _order_day(by_day[d])]
    stretches, cur, last_arrival = [], None, None
    for m in line:
        d = m["date"]
        pto = home.on(d)[0]
        pfrom = pto + [p for p in home.on(d - _ONE)[0] if p not in pto]  # slept at the old home the night before
        from_uid, to_uid = _home_uid(m["frm"], pfrom), _home_uid(m["to"], pto)
        fh, th = from_uid is not None, to_uid is not None
        if fh and th:
            if cur is not None:
                cur["hi"] = d
                stretches.append(cur)
                cur = None
            moved = from_uid != to_uid
            excluded.append({"record_id": m["id"], "kind": m["kind"], "disposition": "a_move" if moved else "at_home",
                             "detail": "arrives at a new residence on or after its first night" if moved
                             else "between places that count as home"})
            last_arrival = d
        elif fh:
            if cur is not None:
                cur["hi"] = d
                stretches.append(cur)
            cur = _stretch(m, True, last_arrival, from_uid)
        elif th:
            if cur is not None:
                cur["movs"].append(m)
                cur["end_rec"] = True
                cur["end_rule"] = "a_move" if to_uid != cur["uid"] else None
                stretches.append(cur)
                cur = None
            else:
                s = _stretch(m, False, last_arrival, to_uid)
                s["end_rec"] = True
                stretches.append(s)
            last_arrival = d
        elif cur is None:
            cur = _stretch(m, False, last_arrival, None)
        else:
            cur["movs"].append(m)
    if cur is not None:
        stretches.append(cur)
    return stretches


def _window(s):
    left = s["movs"][0]["date"] if s["start_rec"] else (s["lo"] or _dt.date.min)
    if s["end_rec"]:
        return left, s["movs"][-1]["date"], True
    return left, (s["hi"] or _dt.date.max), s["hi"] is None


def _attach(stretches, presences, home, excluded):
    """Rule one_line: lodging and rentals ride the same line. The first stretch whose window they overlap takes them
    (one container each). Leftovers away from home form presence-only stretches (no_flight_needed); at home, excluded."""
    wins = [_window(s) for s in stretches]
    loose = []
    for p in sorted(presences, key=lambda e: (e["start"], KIND_ORDER[e["kind"]], e["id"])):
        for s, (left, right, incl) in zip(stretches, wins):
            if p["start"] <= (right if incl else right - _ONE) and p["checkout"] >= left:
                s["lodging" if p["kind"] == "lodging" else "rentals"].append(p)
                break
        else:
            loose.append(p)
    extra, ends = [], []
    for p in loose:
        if _home_uid(p["place"], home.on(p["start"])[0]) is not None:
            excluded.append({"record_id": p["id"], "kind": p["kind"], "disposition": "at_home",
                             "detail": "stay or rental at a place that counts as home"})
            continue
        if extra and p["start"] <= ends[-1]:
            ends[-1] = max(ends[-1], p["checkout"])
        else:
            extra.append({"movs": [], "start_rec": False, "end_rec": False, "lo": None, "hi": None, "uid": None,
                          "end_rule": None, "lodging": [], "rentals": []})
            ends.append(p["checkout"])
        extra[-1]["lodging" if p["kind"] == "lodging" else "rentals"].append(p)
    return stretches + extra


# --------------------------------------------------------------------------- boundaries
def _all(s):
    return s["movs"] + s["lodging"] + s["rentals"]


def _evidence(s):
    return (min([m["date"] for m in s["movs"]] + [p["start"] for p in s["lodging"] + s["rentals"]]),
            max([m["date"] for m in s["movs"]] + [p["checkout"] for p in s["lodging"] + s["rentals"]]))


def _start(s):
    """recorded -> from_lodging -> forced -> bounded -> undecided. -> (date, basis, rule)"""
    if s["start_rec"]:
        return s["movs"][0]["date"], "recorded", None
    if s["lodging"]:
        ci = min(p["start"] for p in s["lodging"])
        if not s["movs"] or ci <= s["movs"][0]["date"]:
            return ci, "derived", "from_lodging"
    if s["lo"] is not None and s["lo"] == s["first_ev"]:
        return s["first_ev"], "derived", "forced"
    if s["lo"] is not None:
        return s["lo"], "undecided", "bounded"
    return s["first_ev"], "undecided", None


def _finish(s):
    if s["end_rec"]:
        return s["movs"][-1]["date"], ("derived" if s["end_rule"] else "recorded"), s["end_rule"]
    if s["lodging"]:
        co = max(p["checkout"] for p in s["lodging"])
        if co >= s["last_ev"] and (s["hi"] is None or co <= s["hi"]):
            return co, "derived", "from_lodging"
    if s["hi"] is not None and s["hi"] == s["last_ev"]:
        return s["last_ev"], "derived", "forced"
    if s["hi"] is not None:
        return s["hi"], "undecided", "bounded"
    return s["last_ev"], "undecided", None


def _places_visited(s):
    """Rule connection_or_visit: a place left the day it was reached is a connection, not a place visited."""
    movs, vis = s["movs"], []
    for i, m in enumerate(movs):
        last = i + 1 == len(movs)
        if (not last and movs[i + 1]["date"] == m["date"]) or (last and s["end_rec"]):
            continue
        vis.append(m["to"])
    if not movs:
        vis = [p["place"] for p in sorted(s["lodging"] + s["rentals"], key=lambda e: (e["start"], e["id"]))]
    out = []
    for e in vis:
        k = e.get("area_id") or e.get("place_id")
        if k and (not out or out[-1] != k):
            out.append(k)
    return out


# --------------------------------------------------------------------------- the entry point
def compute_trips(records, home_periods, *, today, home_places=(), corrections=(), near_home_km=50.0):
    today = _d(today)
    if today is None:
        raise ValueError("today is required; the engine never reads the clock")
    home = _Home(list(home_periods), list(home_places))
    corr = sorted((c for c in corrections if c.get("status", "active") == "active"),
                  key=lambda c: (str(c.get("kind")), str(c.get("date")), ",".join(sorted(c.get("record_ids") or [])),
                                 str(c.get("created_at"))))
    left_out = {i for c in corr if c.get("kind") == "leave_out" for i in (c.get("record_ids") or [])}

    excluded, seen, events, repeated = [], set(), [], []
    for rec in sorted(records, key=lambda r: (_rid(r), str(r.get("status")), str(r.get("date_iso") or r.get("first_night")))):
        rid, kind = _rid(rec), _kind_of(rec)
        why = None
        if rid in seen:  # the same id again is the same record: counted once, reported in duplicate_inputs
            repeated.append(rid)
            continue
        if rec.get("status") == "cancelled":
            why = ("cancelled", "status cancelled")
        elif rid in left_out:
            why = ("left_out", "a leave_out correction")
        seen.add(rid)
        ev = None if why else _normalise(rec)
        if why is None and ev is None:
            why = ("unreadable", "missing or invalid fields")
        if why:
            excluded.append({"record_id": rid, "kind": kind, "disposition": why[0], "detail": why[1]})
        else:
            events.append(ev)

    movs = [e for e in events if e["kind"] in ("flight", "drive")]
    pres = [e for e in events if e["kind"] in ("lodging", "car_rental")]
    stretches = _attach(_cut(movs, home, excluded), pres, home, excluded)
    for s in stretches:
        s["first_ev"], s["last_ev"] = _evidence(s)
    stretches.sort(key=lambda s: (s["first_ev"], min(e["id"] for e in _all(s))))
    stretches, applied = _merge_together(stretches, corr)

    trips = []
    for s in stretches:
        trip = _finalize(s, home, today, corr, applied, near_home_km)
        if trip is None:
            excluded.extend({"record_id": e["id"], "kind": e["kind"], "disposition": "day_out_and_back",
                             "detail": "left and arrived home on the same day"} for e in _all(s))
        else:
            trips.append(trip)
    trips.sort(key=lambda t: (t["first_date"], t["key"]))
    _signals(trips)
    excluded.sort(key=lambda x: (x["record_id"], x["disposition"]))
    unapplied = [c for c in corr if c.get("kind") != "leave_out" and id(c) not in applied]
    return {"trips": trips, "excluded": excluded, "unapplied_corrections": unapplied,
            "duplicate_inputs": sorted(set(repeated))}


def _merge_together(stretches, corr):
    """A `together` correction joins the stretches holding its records into one trip."""
    applied, out = set(), list(stretches)
    for c in corr:
        if c.get("kind") != "together":
            continue
        ids = set(c.get("record_ids") or [])
        hit = [s for s in out if {e["id"] for e in _all(s)} & ids]
        if hit:
            applied.add(id(c))
        if len(hit) < 2:
            continue
        hit.sort(key=lambda s: (s["first_ev"], min(e["id"] for e in _all(s))))
        a, z = hit[0], hit[-1]
        merged = {"movs": sorted([m for s in hit for m in s["movs"]], key=lambda m: (m["date"], m["seq"], m["time"], m["id"])),
                  "lodging": [p for s in hit for p in s["lodging"]], "rentals": [p for s in hit for p in s["rentals"]],
                  "start_rec": a["start_rec"], "lo": a["lo"], "uid": a["uid"],
                  "end_rec": z["end_rec"], "hi": z["hi"], "end_rule": z["end_rule"]}
        merged["first_ev"], merged["last_ev"] = _evidence(merged)
        out = [s for s in out if all(s is not h for h in hit)] + [merged]
        out.sort(key=lambda s: (s["first_ev"], min(e["id"] for e in _all(s))))
    return out, applied


def _finalize(s, home, today, corr, applied, near_km):
    ids = {e["id"] for e in _all(s)}
    fd, fb, fr = _start(s)
    ld, lb, lr = _finish(s)
    mine = []
    for c in corr:
        if not ids & set(c.get("record_ids") or []):
            continue
        if c.get("kind") == "returned" and _d(c.get("date")):
            ld, lb, lr = _d(c["date"]), "recorded", None
        elif c.get("kind") == "left" and _d(c.get("date")):
            fd, fb, fr = _d(c["date"]), "recorded", None
        elif c.get("kind") != "together":
            continue
        mine.append(c)
        applied.add(id(c))
    if fb != "undecided" and lb != "undecided" and fd == ld:
        return None  # a day out and back is no trip
    recs = sorted(_all(s), key=lambda e: (e["date"], KIND_ORDER[e["kind"]], e.get("seq", 0), e["id"]))
    flags = []
    if fb == "undecided":
        flags.append("start not recorded")
    if lb == "undecided":
        flags.append("return not planned yet" if s["last_ev"] >= today and s["hi"] is None else "return not recorded")
    if any(not home.on(x)[1] for e in recs for x in (e["date"], e.get("checkout") or e["date"])):
        flags.append("home not recorded for these dates")
    if _near_unlisted(s, home, near_km):
        flags.append("is this place home")
    per = home.on(fd)[0][0]
    return {
        "key": recs[0]["id"], "first_date": _iso(fd), "first_basis": fb, "first_rule": fr,
        "last_date": _iso(ld), "last_basis": lb, "last_rule": lr, "nights": max((ld - fd).days, 0),
        "nights_exact": fb != "undecided" and lb != "undecided",
        "first_evidence_date": _iso(s["first_ev"]), "last_evidence_date": _iso(s["last_ev"]),
        "places_visited": _places_visited(s),
        "legs": [{"record_id": m["id"], "kind": m["kind"], "date": _iso(m["date"]),
                  "from": m["frm"]["place_id"] or m["frm"]["area_id"], "to": m["to"]["place_id"] or m["to"]["area_id"]}
                 for m in s["movs"]],
        "stays": [{"record_id": p["id"], "first_night": _iso(p["start"]), "checkout": _iso(p["checkout"])}
                  for p in sorted(s["lodging"], key=lambda e: (e["start"], e["id"]))],
        "rentals": [{"record_id": p["id"], "pickup_date": _iso(p["start"]), "dropoff_date": _iso(p["checkout"])}
                    for p in sorted(s["rentals"], key=lambda e: (e["start"], e["id"]))],
        "state": _state(fd, lb, ld, s, today), "flags": flags, "signals": [],
        "residence_period": {k: per["src"].get(k) for k in
                             ("residence_uid", "city", "state", "country", "first_night", "last_night", "tenure", "evidence")},
        "corrections": [{"kind": c.get("kind"), "date": c.get("date"), "record_ids": sorted(c.get("record_ids") or []),
                         "author": c.get("author"), "question_id": c.get("question_id")} for c in mine],
        "record_ids": [e["id"] for e in recs],
    }


def _near_unlisted(s, home, km):
    """A place with coordinates that is not a listed home place, but lies within km of one: 'is this place home'."""
    for e in _all(s):
        ends = [e["place"]] if "place" in e else ([e["frm"], e["to"]] if e["kind"] == "drive" else [])
        pers = home.on(e["date"])[0]
        for end in ends:
            if None in (end.get("lat"), end.get("lon")) or _home_uid(end, pers) is not None:
                continue
            if any(None not in (hp.get("lat"), hp.get("lon")) and _hav((end["lat"], end["lon"]), (hp["lat"], hp["lon"])) <= km
                   for p in pers for hp in p["places"]):
                return True
    return False


def _state(fd, lb, ld, s, today):
    if fd > today:
        return "upcoming" if lb != "undecided" else "planned"
    if ld >= today or (lb == "undecided" and s["hi"] is None and s["last_ev"] >= today):
        return "under way"
    return "past"


def _signals(trips):
    """likely duplicate: two movements on one route within a day. unusually long: a complete trip over 3x the
    ninth decile of complete trips' nights, once there are ten to measure against."""
    legs = sorted(((l["from"] or "", l["to"] or "", l["date"], t["key"], l["record_id"]) for t in trips for l in t["legs"]))
    dup = set()
    for a, b in zip(legs, legs[1:]):
        if a[:2] == b[:2] and a[4] != b[4] and abs((_d(a[2]) - _d(b[2])).days) <= 1:
            dup.update((a[3], b[3]))
    nights = sorted(t["nights"] for t in trips if t["nights_exact"])
    limit = 3 * nights[min(len(nights) - 1, int(0.9 * len(nights)))] if len(nights) >= 10 else None
    for t in trips:
        if t["key"] in dup:
            t["signals"].append("likely duplicate")
        if limit is not None and t["nights_exact"] and t["nights"] > limit:
            t["signals"].append("unusually long")


def order_trips_for_listing(trips):
    """BRD list_trips order: under way, upcoming, planned (soonest first), then past newest first."""
    live = sorted((t for t in trips if t["state"] != "past"), key=lambda t: (STATE_ORDER[t["state"]], t["first_date"], t["key"]))
    past = sorted((t for t in trips if t["state"] == "past"), key=lambda t: (t["first_date"], t["key"]), reverse=True)
    return live + past


def trips_in_range(trips, date_from=None, date_to=None):
    """Trips overlapping [date_from, date_to] (ISO dates, both optional, inclusive)."""
    return [t for t in trips if (not date_to or t["first_date"] <= date_to) and (not date_from or t["last_date"] >= date_from)]
