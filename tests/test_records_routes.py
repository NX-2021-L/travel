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
    assert d["class"] == "derived" and d["order"] == "fixed" and d["capabilities"] == [] and d["inputs"] == [{"external": "travel_records", "label": "travel records"}, "trip_corrections"]
    assert [p["id"] for p in d["params"]] == ["today", "near_home_km"]
    assert d["params"][1]["default"] == 50.0
    assert {c["id"] for c in d["columns"]} >= {"key", "first_date", "last_date", "state", "flags"}
    assert all(c["editable"] is False for c in d["columns"])


def test_corrections_descriptor_is_amend_with_audit_with_create_and_retire():
    # FLY-TSK-087: the write half (tests/test_records_writes.py). Editable names the create fields; nothing is amended.
    _, _, d = get("/records/recordsets/trip_corrections/descriptor")
    assert d["class"] == "amend_with_audit" and d["capabilities"] == ["create", "retire"] and "inputs" not in d
    assert d["reason"] == "required" and "amend" not in d["capabilities"]


def test_trips_rows_are_the_engine_in_listing_order_with_lineage():
    status, _, body = get("/records/recordsets/trips/rows")
    assert status == 200 and set(body) == {"rows", "vintage", "stale", "findings", "next_cursor"}
    assert len(body["rows"]) == 2
    assert [r["values"]["state"] for r in body["rows"]] == ["upcoming", "past"]  # order_trips_for_listing: live, then past
    for r in body["rows"]:
        assert r["sources"] == [{"recordset": rr.SOURCE_SET, "key": rid} for rid in r["values"]["record_ids"]]
        assert r["source"] == r["sources"][0]  # the deprecated alias is the first source
        assert set(r["values"]) == {c["id"] for c in rr.TRIP_COLUMNS}
    assert body["stale"] is None and body["next_cursor"] is None


def test_trips_findings_carry_exclusions_unapplied_and_duplicates():
    _, _, body = get("/records/recordsets/trips/rows")
    assert {f["code"] for f in body["findings"]} == {"excluded_record", "unapplied_correction", "duplicate_input"}
    # FLY-TSK-089 (lead ruling 18:09Z): one finding per code, a short summary; no rows (they are not trip rows, REC-C44)
    by = {f["code"]: f for f in body["findings"]}
    assert len(body["findings"]) == 3 and all("rows" not in f for f in body["findings"])
    # Finding.refs (contract 0.6.0): the records each finding speaks of, in the sets they belong to
    assert by["excluded_record"]["refs"] == [{"recordset": "travel_records", "key": "F-GONE"}]
    assert by["unapplied_correction"]["refs"] == [{"recordset": "trip_corrections", "key": "C-GHOST"}]
    assert by["duplicate_input"]["refs"] == [{"recordset": "travel_records", "key": "F-OUT1"}]
    assert by["excluded_record"]["message"] == "1 cancelled flight is in no trip"
    assert by["unapplied_correction"]["message"] == "1 correction changed no trip; 1 names records that are not held"
    assert by["duplicate_input"]["message"] == "1 record was supplied twice and counted once"
    assert all(f["severity"] in ("info", "warn", "error") for f in body["findings"])


def test_findings_summarise_many_records_in_one_line_per_code():
    ex = [{"record_id": f"F-X{i}", "kind": "flight", "disposition": "cancelled", "detail": ""} for i in range(3)]
    ex += [{"record_id": f"drive-{i}", "kind": "drive", "disposition": "at_home", "detail": ""} for i in range(2)]
    out = rr._findings({"excluded": ex, "unapplied_corrections": [{"correction_id": "C-A"}, {"correction_id": "C-B"}],
                        "duplicate_inputs": ["F-X0", "F-X1"]})
    assert [f["message"] for f in out] == ["5 records are in no trip: 3 cancelled flights, 2 drives at home",
                                           "2 corrections changed no trip", "2 records were supplied twice and counted once"]
    only = rr._findings({"excluded": ex[:3], "unapplied_corrections": [], "duplicate_inputs": []})
    assert [f["message"] for f in only] == ["3 cancelled flights are in no trip"]


def test_labels_are_lowercase_at_the_source_and_params_carry_labels():
    for d in rr.descriptors().values():
        assert d["title"] == d["title"].lower() and d["description"] == d["description"].lower()
        for c in d["columns"]:
            assert c["label"] == c["label"].lower()
            assert all(o["label"] == o["label"].lower() for o in c.get("options", []))
    assert rr.PARAM_LABELS == {"today": "today", "near_home_km": "near home, km"}
    # contract 0.6.0 (lane 2H): Param.label
    assert {p["id"]: p["label"] for p in rr.descriptors()["trips"]["params"]} == rr.PARAM_LABELS


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
    # FLY-TSK-089: a retired correction is listed with Row.retired true; the row carries no status column
    assert [r.get("retired", False) for r in body["rows"]] == [False, False, True]
    assert all("status" not in r["values"] for r in body["rows"])
    assert all(r["source"] is None and r["sources"] == [] for r in body["rows"]) and body["findings"] == []


def test_unknown_set_and_unknown_route_are_404_problems():
    for path in ("/records/recordsets/nope/descriptor", "/records/recordsets/nope/writes", "/records/other",
                 "/records/recordsets/trips/intents"):
        status, ctype, _ = get(path)
        assert status == 404 and ctype == "application/problem+json", path


def test_a_derived_sets_writes_log_is_empty_not_missing():
    # INT-PLN-031's live check: every served set answers its writes route; trips is computed and has no writes
    status, ctype, body = get("/records/recordsets/trips/writes")
    assert status == 200 and ctype == "application/json" and body == []


def test_trips_take_no_writes():
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


def test_flags_and_signals_are_hidden_by_default():
    # FLY-TSK-089, orchestration 18:18Z: the grid opens without them, as the canvas does; they stay in every row
    cols = {c["id"]: c for c in rr.descriptors()["trips"]["columns"]}
    assert cols["flags"]["hint"]["default_hidden"] is True and cols["signals"]["hint"]["default_hidden"] is True
