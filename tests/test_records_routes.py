"""FLY-TSK-083: the read-only /records routes. Offline; synthetic fixtures and a synthetic door key (tests/records_fixture.py)."""
import json
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import records_fixture as fx  # noqa: E402,F401  (puts travel_mcp on sys.path)
try:
    import io_kit_mcp_edge  # noqa: F401
except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
    _stub = types.ModuleType("io_kit_mcp_edge")
    for _n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
        setattr(_stub, _n, type(_n, (Exception,), {}))
    _jw = types.ModuleType("io_kit_mcp_edge.jwks")
    _jw.fetch_https = lambda *a, **k: {}
    sys.modules["io_kit_mcp_edge"], sys.modules["io_kit_mcp_edge.jwks"] = _stub, _jw

import contract  # noqa: E402
import lambda_function as lf  # noqa: E402
import records_door  # noqa: E402
import records_routes as rr  # noqa: E402

SCHEMA = None


@pytest.fixture(autouse=True)
def world(monkeypatch):
    for k, v in fx.env().items():
        monkeypatch.setenv(k, v)
    fx.install(monkeypatch)


def get(path, token="good", qs="", method="GET", headers=None):
    h = dict(headers or {})
    if token == "good":
        token = fx.token()
    if token:
        h["Authorization"] = f"Bearer {token}"
    ev = {"requestContext": {"http": {"method": method, "path": path}}, "rawPath": path, "rawQueryString": qs, "headers": h}
    r = rr.handle(ev)
    return r["statusCode"], r["headers"]["content-type"], json.loads(r["body"])


ROUTES = ["/records/recordsets", "/records/recordsets/trips/descriptor", "/records/recordsets/trips/rows",
          "/records/recordsets/trip_corrections/descriptor", "/records/recordsets/trip_corrections/rows"]


def test_list_names_the_two_sets():
    status, ctype, body = get("/records/recordsets")
    assert status == 200 and ctype == "application/json"
    assert [s["id"] for s in body] == ["trips", "trip_corrections"]


def test_trips_descriptor_is_derived_fixed_with_engine_params():
    _, _, d = get("/records/recordsets/trips/descriptor")
    assert d["class"] == "derived" and d["order"] == "fixed" and d["capabilities"] == [] and d["inputs"] == ["trip_corrections"]
    assert [p["id"] for p in d["params"]] == ["today", "near_home_km"]
    assert d["params"][1]["default"] == 50.0
    assert {c["id"] for c in d["columns"]} >= {"key", "first_date", "last_date", "state", "flags"}
    assert all(c["editable"] is False for c in d["columns"])


def test_corrections_descriptor_is_amend_with_audit_and_advertises_no_write():
    _, _, d = get("/records/recordsets/trip_corrections/descriptor")
    assert d["class"] == "amend_with_audit" and d["capabilities"] == [] and "inputs" not in d
    assert all(c["editable"] is False for c in d["columns"])


def test_trips_rows_are_the_engine_in_listing_order_with_lineage():
    status, _, body = get("/records/recordsets/trips/rows")
    assert status == 200 and set(body) == {"rows", "vintage", "stale", "findings", "next_cursor"}
    assert len(body["rows"]) == 2
    assert [r["values"]["state"] for r in body["rows"]] == ["upcoming", "past"]  # order_trips_for_listing: live, then past
    for r in body["rows"]:
        assert r["source"]["recordset"] == rr.SOURCE_SET and r["source"]["key"] == r["values"]["record_ids"]
        assert set(r["values"]) == {c["id"] for c in rr.TRIP_COLUMNS}
    assert body["stale"] is None and body["next_cursor"] is None


def test_trips_findings_carry_exclusions_unapplied_and_duplicates():
    _, _, body = get("/records/recordsets/trips/rows")
    assert {f["code"] for f in body["findings"]} == {"excluded_record", "unapplied_correction", "duplicate_input"}
    assert any("F-GONE" in f["message"] and "cancelled" in f["message"] for f in body["findings"])
    assert any("C-GHOST" in f["message"] and "F-NOWHERE" in f["message"] for f in body["findings"])
    assert any("F-OUT1" in f["message"] for f in body["findings"] if f["code"] == "duplicate_input")
    assert all(f["severity"] in ("info", "warn", "error") for f in body["findings"])


def test_trips_params_change_the_computation():
    _, _, a = get("/records/recordsets/trips/rows")
    _, _, b = get("/records/recordsets/trips/rows", qs="today=2025-01-01")
    assert [r["values"]["state"] for r in b["rows"]] == ["past", "past"]
    assert a["vintage"] != b["vintage"]
    assert get("/records/recordsets/trips/rows", qs="today=2025-01-01")[2] == b  # convergent


def test_bad_params_and_cursors_are_problems():
    for qs, field in (("today=nope", "today"), ("near_home_km=-1", "near_home_km"), ("near_home_km=x", "near_home_km"),
                      ("limit=0", "limit"), ("cursor=bogus", "cursor")):
        status, ctype, body = get("/records/recordsets/trips/rows", qs=qs)
        assert status == 400 and ctype == "application/problem+json" and body["field"] == field, qs


def test_paging_walks_every_row_once_and_a_foreign_cursor_is_refused():
    seen, qs = [], "limit=1"
    while True:
        _, _, page = get("/records/recordsets/trips/rows", qs=qs)
        seen += [r["key"] for r in page["rows"]]
        if not page["next_cursor"]:
            break
        qs = f"limit=1&cursor={page['next_cursor']}"
    assert len(seen) == 2 and len(set(seen)) == 2
    _, _, first = get("/records/recordsets/trips/rows", qs="limit=1")
    assert get("/records/recordsets/trip_corrections/rows", qs=f"cursor={first['next_cursor']}")[0] == 400


def test_corrections_rows_include_retired_and_have_no_source():
    _, _, body = get("/records/recordsets/trip_corrections/rows")
    assert [r["key"] for r in body["rows"]] == ["C-ACTIVE", "C-GHOST", "C-RETIRED"]
    assert {r["values"]["status"] for r in body["rows"]} == {"active", "retired"}
    assert all(r["source"] is None for r in body["rows"]) and body["findings"] == []


def test_unknown_set_and_unknown_route_are_404_problems():
    for path in ("/records/recordsets/nope/descriptor", "/records/recordsets/trips/writes", "/records/other",
                 "/records/recordsets/trips/intents"):
        status, ctype, _ = get(path)
        assert status == 404 and ctype == "application/problem+json", path


def test_writes_are_not_served():
    assert get("/records/recordsets/trips/intents", method="POST")[0] == 405
    assert get("/records/recordsets/trip_corrections/rows", method="DELETE")[0] == 405


# ---------------------------------------------------------------- the door
BAD_TOKENS = {
    "missing": None,
    "api key": "tk_live_0123456789abcdef",
    "connector bearer (RS256-style, other issuer)": None,  # filled below
    "wrong issuer": fx.token(iss=fx.OTHER_ISSUER),
    "wrong audience": fx.token(aud="someone-else"),
    "not human": fx.token(principal_type="agent"),
    "expired": fx.token(exp_in=-5),
    "wrong typ": fx.token(typ="JWT"),
    "wrong key": fx.other_key_token(),
    "no window_opened_at": __import__("jwt").encode({"iss": fx.ISSUER, "aud": fx.AUDIENCE, "sub": "s", "iat": 1, "exp": 4102444800,
                                                      "principal_type": "human"}, fx._KEY, algorithm="ES256", headers={"typ": "door+jwt"}),
}
BAD_TOKENS["connector bearer (RS256-style, other issuer)"] = ".".join(
    __import__("base64").urlsafe_b64encode(json.dumps(p).encode()).decode().rstrip("=")
    for p in ({"alg": "RS256", "typ": "JWT"}, {"iss": "https://idp.example.test/pool", "aud": "c", "token_use": "access"})) + ".sig"


@pytest.mark.parametrize("path", ROUTES)
@pytest.mark.parametrize("why", sorted(BAD_TOKENS))
def test_every_refusal_is_the_same_401_on_every_route(path, why):
    status, ctype, body = get(path, token=BAD_TOKENS[why])
    assert status == 401 and ctype == "application/problem+json" and body["title"] == "Unauthorized"
    assert body == get(path, token=None)[2]  # no reason leaks: all refusals read alike


def test_alg_none_and_hs256_are_refused():
    import jwt
    claims = {"iss": fx.ISSUER, "aud": fx.AUDIENCE, "sub": "s", "iat": 1, "exp": 4102444800, "principal_type": "human", "window_opened_at": 1}
    hs = jwt.encode(claims, "secret" * 8, algorithm="HS256", headers={"typ": "door+jwt"})
    assert get("/records/recordsets", token=hs)[0] == 401


@pytest.mark.parametrize("missing", ["RECORDS_DOOR_ISSUERS", "RECORDS_DOOR_AUDIENCE", "RECORDS_DOOR_PUBLIC_KEYS"])
def test_missing_door_setting_fails_closed(monkeypatch, missing):
    monkeypatch.delenv(missing)
    assert get("/records/recordsets")[0] == 401


def test_second_configured_issuer_is_accepted_only_with_its_own_key(monkeypatch):
    monkeypatch.setenv("RECORDS_DOOR_ISSUERS", f"{fx.ISSUER},{fx.OTHER_ISSUER}")
    assert get("/records/recordsets", token=fx.token(iss=fx.OTHER_ISSUER))[0] == 401  # listed, but no key for it
    assert get("/records/recordsets")[0] == 200


def test_door_reasons_are_named_for_the_log():
    with pytest.raises(records_door.DoorTokenError) as e:
        records_door.authenticate({"authorization": f"Bearer {fx.token(exp_in=-5)}"}, fx.env())
    assert e.value.reason == "door_token_expired"
    with pytest.raises(records_door.DoorTokenError) as e:
        records_door.authenticate({"authorization": f"Bearer {fx.token(principal_type='service')}"}, fx.env())
    assert e.value.reason == "door_token_not_human"


# ---------------------------------------------------------------- ui_only
def test_routes_are_not_tools_and_not_in_caps_or_manifest():
    caps = json.dumps(lf.build_caps()) + json.dumps(lf.build_permission_manifest())
    assert "recordsets" not in caps and "/records" not in caps and "records_routes" not in caps
    assert not any("recordset" in n or n in ("trips", "trip_corrections") for n in contract.TOOL_NAMES)
    committed = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp" / "caps.json"
    assert "recordsets" not in committed.read_text()


def test_lambda_handler_routes_records_before_the_edge(monkeypatch):
    called = []
    monkeypatch.setattr(lf, "_get_edge", lambda: called.append("edge"))
    ev = {"requestContext": {"http": {"method": "GET", "path": "/records/recordsets"}}, "rawPath": "/records/recordsets",
          "headers": {"authorization": f"Bearer {fx.token()}"}}
    if lf.DOOR_MODE != "public":
        pytest.skip("public door only")
    r = lf.lambda_handler(ev, None)
    assert r["statusCode"] == 200 and not called
    r = lf.lambda_handler({**ev, "headers": {}}, None)
    assert r["statusCode"] == 401 and not called


def test_internal_door_never_serves_records():
    from door import HttpEventRefused, admit
    ev = {"requestContext": {"http": {"method": "GET", "path": "/records/recordsets"}}}
    with pytest.raises(HttpEventRefused):
        admit(ev, "internal", "role")
