"""Owner-marker guard for the records surface (FLY-TSK-088, BRD DOC-D14184D324F0 T-36 and 0.1).

io-kit's vocabulary lint covers the kit packages only, so travel guards its own records surface (DVP-PLN-021 lead,
10:19Z): the routes, the door, the synthetic fixture, the local server, the records tests and the conformance runner.

1. No value a deployment supplies appears in any of them. The values are read here from deploy/settings.env and
   deploy/targets/*.env at test time, so this file names none of them.
2. Every person, place, id and reason in the records fixture carries a synthetic marker, so a value copied from a
   live row (a trip, a place, an author) fails here even when no settings file holds it.
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TL = ROOT / "backend" / "lambda" / "travel_mcp"

SURFACE = sorted(
    [TL / n for n in ("records_routes.py", "records_door.py", "records_writes.py") if (TL / n).exists()]
    + [ROOT / "tests" / n for n in ("records_fixture.py", "records_local_server.py")]
    + list((ROOT / "tests").glob("test_records_*.py"))
    + list((ROOT / "tests" / "conformance").glob("*.mjs"))
)


def _supplied_values():
    """Distinctive values from every deployment's settings: whole values, list items, URL hosts, account ids, JSON strings."""
    out = set()
    for env in [ROOT / "deploy" / "settings.env", *sorted((ROOT / "deploy" / "targets").glob("*.env"))]:
        for line in env.read_text().splitlines():
            if line.lstrip().startswith("#") or "=" not in line:
                continue
            value = line.split("=", 1)[1].strip().strip("'\"")
            parts = set(re.split(r"[,\s]+", value)) | set(re.findall(r'"([^"]+)"', value))
            for p in list(parts):
                parts.update(re.findall(r"https?://([^/:\"]+)", p))
                parts.update(re.findall(r"\b\d{12}\b", p))
            out.update(parts)
    # Plain words (a door mode, a region, a repository name) and key material are not distinctive of a deployment.
    return {v for v in out if len(v) >= 8 and not re.fullmatch(r"[a-z]+|[a-z]{2}-[a-z]+-\d", v) and "-----" not in v}


def test_the_surface_is_the_one_this_guard_names():
    names = {p.name for p in SURFACE}
    assert {"records_routes.py", "records_door.py", "records_fixture.py", "run_records_conformance.mjs"} <= names


def test_no_supplied_value_on_the_records_surface():
    values = _supplied_values()
    assert values, "no settings were read"
    hits = [(p.relative_to(ROOT).as_posix(), i) for p in SURFACE for i, line in enumerate(p.read_text().splitlines(), 1)
            if any(v in line for v in values)]
    assert hits == [], f"a deployment's value on the records surface (file, line): {hits}"


# The synthetic markers the records fixture uses. A new fixture value takes one of these, or the guard fails.
SYNTHETIC_ID = re.compile(r"^(F-[A-Z0-9]+|C-[A-Z]+|R\d|apt:[A-Z]{3}|city:[a-z]{3}|fixture-[a-z0-9-]+)$")
SYNTHETIC_TEXT = {"Testville", "tester", "synthetic", "synthetic-sub"}


def test_records_fixture_values_carry_a_synthetic_marker():
    import records_fixture as fx

    ids = [r["flight_id"] for r in fx.RECORDS] + [r[k] for r in fx.RECORDS for k in ("origin_place_id", "dest_place_id",
                                                                                   "origin_area_id", "dest_area_id")]
    ids += [c["correction_id"] for c in fx.CORRECTIONS] + [i for c in fx.CORRECTIONS for i in c["record_ids"]]
    ids += [fx.HOME["residence_uid"]] + [v for p in fx.HOME["places"] for v in p.values()]
    assert [i for i in ids if not SYNTHETIC_ID.match(i)] == []
    people = [c[k] for c in fx.CORRECTIONS for k in ("author", "retired_by", "reason") if c.get(k)] + [fx.HOME["city"]]
    assert [p for p in people if p not in SYNTHETIC_TEXT] == []
    assert fx.ISSUER.endswith(".test") and fx.OTHER_ISSUER.endswith(".test")
    claims = json.loads(__import__("base64").urlsafe_b64decode(fx.token().split(".")[1] + "=="))
    assert claims["sub"] in SYNTHETIC_TEXT
