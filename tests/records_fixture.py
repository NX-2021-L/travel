"""Synthetic world for the /records routes (FLY-TSK-083): fake records, a fake door key, a patched trip source.

No person's data: every id, place and date is invented. `install(monkeypatch=None)` replaces the two reads the routes
make (lane_3a_trips._compute, lane_2e_corrections.load_corrections) with the REAL trip engine over these records, so the
routes serve real engine output. Used by tests/test_records_routes.py and tests/records_local_server.py.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))

import jwt  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402

ISSUER, OTHER_ISSUER, AUDIENCE = "https://door.example.test", "https://other.example.test", "records-test-audience"
TODAY = "2024-12-01"
HOME = {"residence_uid": "R1", "first_night": "2020-01-01", "last_night": None, "city": "Testville",
        "places": [{"place_id": "apt:AAA", "area_id": "city:aaa"}]}
AAA, BBB, CCC = ("apt:AAA", "city:aaa"), ("apt:BBB", "city:bbb"), ("apt:CCC", "city:ccc")


def _fl(fid, date, o, d, **kw):
    return {"flight_id": fid, "date_iso": date, "origin": o[0][4:], "dest": d[0][4:], "origin_place_id": o[0],
            "origin_area_id": o[1], "dest_place_id": d[0], "dest_area_id": d[1], "status": "active", **kw}


RECORDS = [
    _fl("F-OUT1", "2024-10-01", AAA, BBB), _fl("F-BACK1", "2024-10-08", BBB, AAA),
    _fl("F-OUT2", "2024-12-10", AAA, CCC), _fl("F-BACK2", "2024-12-15", CCC, AAA),
    _fl("F-GONE", "2024-11-01", AAA, BBB, status="cancelled"),
    _fl("F-OUT1", "2024-10-01", AAA, BBB),  # the same id again: duplicate_inputs
]
CORRECTIONS = [
    {"correction_id": "C-ACTIVE", "kind": "returned", "status": "active", "date": "2024-10-09", "date_iso": "2024-10-09",
     "record_ids": ["F-BACK1"], "reason": "synthetic", "author": "tester", "session": None, "question_id": None,
     "created_at": "2024-10-10T00:00:00Z", "retired_at": None, "retired_by": None},
    {"correction_id": "C-GHOST", "kind": "returned", "status": "active", "date": "2024-10-09", "date_iso": "2024-10-09",
     "record_ids": ["F-NOWHERE"], "reason": "synthetic", "author": "tester", "session": None, "question_id": None,
     "created_at": "2024-10-11T00:00:00Z", "retired_at": None, "retired_by": None},
    {"correction_id": "C-RETIRED", "kind": "left", "status": "retired", "date": "2024-10-01", "date_iso": "2024-10-01",
     "record_ids": ["F-OUT1"], "reason": "synthetic", "author": "tester", "session": None, "question_id": None,
     "created_at": "2024-10-12T00:00:00Z", "retired_at": "2024-10-13T00:00:00Z", "retired_by": "tester"},
]

_KEY = ec.generate_private_key(ec.SECP256R1())
_OTHER_KEY = ec.generate_private_key(ec.SECP256R1())
PUBLIC_PEM = _KEY.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()


def env(issuers=(ISSUER,), audience=AUDIENCE):
    return {"RECORDS_DOOR_ISSUERS": ",".join(issuers), "RECORDS_DOOR_AUDIENCE": audience,
            "RECORDS_DOOR_PUBLIC_KEYS": json.dumps({ISSUER: PUBLIC_PEM})}


def token(*, iss=ISSUER, aud=AUDIENCE, principal_type="human", exp_in=300, typ="door+jwt", key=None, alg="ES256", **extra):
    now = int(time.time())
    claims = {"iss": iss, "aud": aud, "sub": "synthetic-sub", "iat": now, "exp": now + exp_in,
              "principal_type": principal_type, "window_opened_at": now, **extra}
    return jwt.encode(claims, key or _KEY, algorithm=alg, headers={"typ": typ})


def other_key_token(**kw):
    return token(key=_OTHER_KEY, **kw)


def install(monkeypatch=None):
    """Patch the two reads; returns an undo callable (or uses monkeypatch when given)."""
    import lane_2e_corrections
    import lane_3a_trips
    import trip_engine

    async def compute(today=None, near_home_km=None):
        from datetime import date
        used = today or date.fromisoformat(TODAY)
        extra = {} if near_home_km is None else {"near_home_km": near_home_km}
        active = [c for c in CORRECTIONS if c["status"] == "active"]
        return trip_engine.compute_trips(RECORDS, [HOME], today=used, corrections=active, **extra), used

    def load(table=None, include_retired=False):
        return [c for c in CORRECTIONS if include_retired or c["status"] == "active"]

    patches = [(lane_3a_trips, "_compute", compute), (lane_3a_trips, "_today", lambda: __import__("datetime").date.fromisoformat(TODAY)),
               (lane_2e_corrections, "load_corrections", load)]
    if monkeypatch is not None:
        for mod, name, fn in patches:
            monkeypatch.setattr(mod, name, fn)
        return None
    old = [(m, n, getattr(m, n)) for m, n, _ in patches]
    for mod, name, fn in patches:
        setattr(mod, name, fn)
    return lambda: [setattr(m, n, f) for m, n, f in old]
