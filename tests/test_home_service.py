"""A-8 (FLY-TSK-029-0C): the call to finance and its three failure behaviours, against a fixture.

Offline: the HTTP layer is a stub; no network, no AWS, no real finance call. home_service imports only
contract (stdlib otherwise), so these run without io_kit_mcp_edge.
"""
import inspect
import json
import os
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import contract as c  # noqa: E402
import home_service as hs  # noqa: E402

FIXTURE = json.loads((ROOT / "tests" / "fixtures" / "finance_residence_periods_fixture.json").read_text())
URL = "https://finance.example.test/"
SECRET = "test-bearer-do-not-log"


def envelope(inner, is_error=False):
    return json.dumps({"jsonrpc": "2.0", "id": 1,
                       "result": {"content": [{"type": "text", "text": json.dumps(inner)}], "isError": is_error}}).encode()


class Stub:
    def __init__(self, status=200, body=None, exc=None, seq=None):
        self.calls, self.status, self.body, self.exc, self.seq = [], status, body, exc, seq

    def __call__(self, url, headers, body, timeout):
        self.calls.append((url, headers, json.loads(body), timeout))
        if self.seq:
            return self.seq.pop(0)
        if self.exc:
            raise self.exc
        return self.status, self.body


def ok(periods=None):
    inner = json.loads(json.dumps(FIXTURE))
    if periods is not None:
        inner["result"]["periods"] = periods
    return Stub(200, envelope(inner))


def read(stub, a, b):
    return hs.read_home(a, b, transport=stub, credential=lambda: SECRET, endpoint=URL)


def refusal(stub, a="2021-02-01", b="2021-03-01"):
    with pytest.raises(hs.HomeError) as e:
        read(stub, a, b)
    return e.value


# ---------------------------------------------------------------- clause 1: the trips read sends no home

def test_signature_takes_no_home_and_no_tool():
    params = inspect.signature(hs.read_home).parameters
    assert [n for n, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD] == ["date_from", "date_to"]
    assert not [n for n in params if any(w in n.lower() for w in ("home", "residence", "period", "place", "tool", "name", "method"))]
    for n in ("transport", "credential", "endpoint"):
        assert params[n].kind is inspect.Parameter.KEYWORD_ONLY
    with pytest.raises(TypeError):
        hs.read_home("2021-02-01", "2021-03-01", home="res-a", transport=ok(), credential=lambda: SECRET, endpoint=URL)


def test_request_carries_only_the_window_and_the_fixed_envelope():
    s = ok()
    read(s, "2021-02-01", "2021-03-01")
    url, headers, body, _ = s.calls[0]
    assert url == URL
    assert body == {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "finance_residence_periods",
                               "arguments": {"from": "2021-02-01", "to": "2021-03-01"}}}
    assert headers["Authorization"] == f"Bearer {SECRET}" and headers["X-Agent-Session"] == "io-travel"


# ---------------------------------------------------------------- clause 5: read scoping

def test_only_the_residence_tool_can_be_sent():
    assert hs.RESIDENCE_TOOL == "finance_residence_periods"
    assert inspect.signature(hs._request_body).parameters.keys() == {"date_from", "date_to"}
    src = (D / "home_service.py").read_text()
    for forbidden in ("finance_query", "finance.query", "record_residence", "close_residence", "create_residence"):
        assert forbidden not in src
    # whatever the dates, the one tool is sent, with exactly the two arguments
    for a, b in [("2021-02-01", "2021-02-02"), ("2024-03-01", "2024-03-09")]:
        s = ok()
        read(s, a, b)
        assert s.calls[0][2]["params"]["name"] == hs.RESIDENCE_TOOL
        assert set(s.calls[0][2]["params"]["arguments"]) == {"from", "to"}
    # no other public callable can talk to finance: the entry point is the only public function
    public = [n for n, o in vars(hs).items() if inspect.isfunction(o) and not n.startswith("_") and o.__module__ == "home_service"]
    assert public == ["read_home"]


def test_projection_drops_everything_but_the_narrow_fields():
    dirty = [{"effective_from": "2024-01-01", "effective_to": None, "end_status": "ongoing", "residence_uid": "res-d",
              "tenure": "owned", "state": "S4", "country": "ZZ", "label": "PLACEHOLDER-LABEL", "street": "PLACEHOLDER-STREET",
              "notes": "n", "postal_code": "00000"}]
    r = read(ok(dirty), "2024-02-01", "2024-02-05")
    assert {f for f in vars(r.periods[0])} == {"effective_from", "effective_to", "end_status", "residence_uid", "tenure", "state", "country"}
    assert "PLACEHOLDER" not in repr(r)


def test_credential_is_never_in_errors_or_reprs():
    for stub in (Stub(401, b"nope"), Stub(exc=OSError(f"connect {SECRET}")), ok([])):
        e = refusal(stub)
        assert SECRET not in str(e) and SECRET not in repr(e) and SECRET not in e.cause


def test_no_credential_setting_declared_so_default_refuses_and_reads_no_env(monkeypatch):
    assert not [k for k in c.SETTINGS if "FINANCE" in k and k != "FINANCE_READ_FUNCTION"]
    monkeypatch.setenv("FINANCE_READ_FUNCTION", URL)
    s = ok()
    with pytest.raises(hs.HomeError) as e:
        hs.read_home("2021-02-01", "2021-03-01", transport=s)
    assert e.value.code == "HOME_UNAVAILABLE" and e.value.cause == "config" and not s.calls


def test_endpoint_defaults_to_declared_setting(monkeypatch):
    monkeypatch.setenv(hs.ENDPOINT_SETTING, URL)
    s = ok()
    hs.read_home("2021-02-01", "2021-03-01", transport=s, credential=lambda: SECRET)
    assert s.calls[0][0] == URL
    monkeypatch.setenv(hs.ENDPOINT_SETTING, "arn:aws:lambda:us-west-2:1:function:finance-mcp")  # direct invoke is refused by finance
    with pytest.raises(hs.HomeError) as e:
        hs.read_home("2021-02-01", "2021-03-01", transport=ok(), credential=lambda: SECRET)
    assert e.value.code == "HOME_UNAVAILABLE" and e.value.cause == "config"


# ---------------------------------------------------------------- clause 2: finance unreachable -> HOME_UNAVAILABLE
# The finding's s6 rows 1-10 and 14.

def tool_err(code):
    return Stub(200, envelope({"success": False, "tool": "finance.residence_periods", "error": {"code": code, "message": "m"}}, True))


UNAVAILABLE = {
    "1_transport_oserror": lambda: Stub(exc=OSError("dns")),
    "1_timeout": lambda: Stub(exc=TimeoutError("t")),
    "1_http_500": lambda: Stub(500, b""),
    "1_http_502": lambda: Stub(502, b"bad gateway"),
    "1_http_503": lambda: Stub(503, b""),
    "2_http_429": lambda: Stub(429, b""),
    "3_http_401": lambda: Stub(401, b"unauthorized"),
    "4_http_403": lambda: Stub(403, b"forbidden"),
    "5_http_400_parse": lambda: Stub(400, b'{"jsonrpc":"2.0","error":{"code":-32700}}'),
    "6_http_400_invalid_request": lambda: Stub(400, b'{"error":{"code":-32600}}'),
    "6_http_405": lambda: Stub(405, b""),
    "7_unknown_tool": lambda: tool_err("unknown_tool"),
    "8_invalid_input": lambda: tool_err("invalid_input"),
    "9_storage_conflict_twice": lambda: tool_err("storage_conflict"),
    "10_backend_error": lambda: tool_err("backend_error"),
    "10_jsonrpc_minus_32603": lambda: Stub(200, json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": -32603}}).encode()),
    "10_http_500_body": lambda: Stub(500, b'{"error":"x"}'),
    "10_unrecognised_error_code": lambda: tool_err("something_new"),
    "14_not_json": lambda: Stub(200, b"<html>"),
    "14_envelope_empty_content": lambda: Stub(200, json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"content": [], "isError": False}}).encode()),
    "14_inner_not_json": lambda: Stub(200, json.dumps({"result": {"content": [{"type": "text", "text": "x"}]}}).encode()),
    "14_success_missing": lambda: Stub(200, envelope({"tool": "t", "result": {"periods": []}})),
    "14_periods_not_list": lambda: Stub(200, envelope({"success": True, "result": {"periods": {}}})),
    "14_result_missing": lambda: Stub(200, envelope({"success": True})),
}


@pytest.mark.parametrize("name", sorted(UNAVAILABLE))
def test_unavailable(name):
    e = refusal(UNAVAILABLE[name]())
    assert e.code == "HOME_UNAVAILABLE" and e.status == c.ERROR_CODES["HOME_UNAVAILABLE"] == 503
    assert e.code in c.ERROR_CODES and e.code != "HOME_UNAUTHORIZED"


def _period(**kw):
    p = {"effective_from": "2024-01-01", "effective_to": None, "end_status": "ongoing", "residence_uid": "r", "tenure": "owned",
         "state": None, "country": None}
    p.update(kw)
    return p


@pytest.mark.parametrize("bad", [
    {"effective_from": None}, {"effective_from": "2024-1-1"}, {"effective_from": "2024-13-40"}, {"effective_from": 20240101},
    {"end_status": "forever"}, {"end_status": None},
    {"end_status": "ended", "effective_to": None},
    {"end_status": "ended", "effective_to": "2023-12-31"},
    {"end_status": "ended", "effective_to": "2024-01-01"},
    {"end_status": "ended", "effective_to": "soon"},
    {"end_status": "ongoing", "effective_to": "2025-01-01"},
    {"end_status": "end_unknown", "effective_to": "2025-01-01"},
    {"residence_uid": None}, {"tenure": 3}, {"state": 4},
])
def test_14_malformed_period_is_unavailable_not_guessed(bad):
    e = refusal(ok([_period(**bad)]), "2024-02-01", "2024-02-05")
    assert e.code == "HOME_UNAVAILABLE" and e.cause == "contract_violation" and e.alert


def test_14_period_not_object():
    assert refusal(ok(["x"])).code == "HOME_UNAVAILABLE"


def test_9_storage_conflict_retried_once_then_succeeds():
    good = (200, envelope(FIXTURE))
    conflict = (200, envelope({"success": False, "error": {"code": "storage_conflict"}}, True))
    s = Stub(seq=[conflict, good])
    assert read(s, "2021-02-01", "2021-03-01").flags == () and len(s.calls) == 2
    s = Stub(seq=[conflict, conflict])
    assert refusal(s).code == "HOME_UNAVAILABLE" and len(s.calls) == 2


def test_alerting_tags_distinguish_bugs_and_config_from_outages():
    assert refusal(Stub(401, b"")).alert and refusal(Stub(403, b"")).alert and refusal(Stub(400, b"")).alert
    assert refusal(tool_err("invalid_input")).alert
    assert not refusal(Stub(503, b"")).alert and not refusal(Stub(429, b"")).alert and not refusal(tool_err("unknown_tool")).alert


def test_bad_window_is_a_caller_error_not_a_home_code():
    for a, b in [("2021-03-01", "2021-02-01"), ("x", "2021-02-01"), ("2021-02-01", None)]:
        with pytest.raises(ValueError):
            read(ok(), a, b)


# ---------------------------------------------------------------- clause 3: no period -> NO_HOME_RECORDED

def test_11_empty_list_refuses():
    e = refusal(ok([]))
    assert e.code == "NO_HOME_RECORDED" and e.status == 422


def test_12_window_wholly_outside_periods_refuses():
    assert refusal(ok(), "2019-01-01", "2019-12-31").code == "NO_HOME_RECORDED"  # before the first period
    assert refusal(ok(), "2023-07-01", "2023-08-01").code == "NO_HOME_RECORDED"  # wholly inside the gap


def test_ended_end_is_exclusive():
    r = read(ok(), "2023-06-14", "2023-06-14")  # last night
    assert r.flags == () and r.period_for(date(2023, 6, 14)).residence_uid == "res-b"
    assert refusal(ok(), "2023-06-15", "2023-06-15").code == "NO_HOME_RECORDED"  # move-out night is not home


def test_13_end_unknown_covers_only_its_first_night():
    assert read(ok(), "2023-09-01", "2023-09-01").flags == ()
    e = refusal(ok(), "2023-09-02", "2024-01-01")  # the span after effective_from
    assert e.code == "NO_HOME_RECORDED"
    only = [_period(effective_from="2023-09-01", end_status="end_unknown")]
    assert refusal(ok(only), "2024-06-01", "2024-06-30").code == "NO_HOME_RECORDED"  # not "home until today"
    assert refusal(ok(only), "2023-09-02", "2023-09-02").code == "NO_HOME_RECORDED"


def test_ongoing_is_open_ended():
    r = read(ok(), "2025-01-01", "2026-12-31")
    assert r.flags == () and r.period_for(date(2026, 12, 31)).end_status == "ongoing"


# ---------------------------------------------------------------- clause 4: uncovered dates flag, not refuse

FLAG = "home not recorded for these dates"


def test_flag_is_from_the_contract_vocabulary():
    assert hs.NOT_RECORDED_FLAG == FLAG and FLAG in c.FLAGS


def d(s):
    return date.fromisoformat(s)


@pytest.mark.parametrize("a,b,spans", [
    ("2023-06-01", "2023-07-01", [("2023-06-15", "2023-07-01")]),                 # trailing edge into the gap
    ("2023-08-20", "2023-09-05", [("2023-08-20", "2023-08-31"), ("2023-09-02", "2023-09-05")]),  # gap, then end_unknown tail
    ("2020-12-30", "2021-01-02", [("2020-12-30", "2020-12-31")]),                 # leading window edge
    ("2023-06-10", "2024-02-20", [("2023-06-15", "2023-08-31"), ("2023-09-02", "2024-02-09")]),  # gap + unknown span
    ("2022-02-28", "2024-02-10", [("2023-06-15", "2023-08-31"), ("2023-09-02", "2024-02-09")]),
])
def test_partial_coverage_flags_and_reports_spans(a, b, spans):
    r = read(ok(), a, b)
    assert r.flags == (FLAG,)
    assert r.uncovered == tuple((d(x), d(y)) for x, y in spans)


@pytest.mark.parametrize("a,b", [
    ("2021-02-01", "2021-03-01"),   # inside one ended period
    ("2022-02-28", "2022-03-01"),   # contiguous periods, gap_days 0, not a gap
    ("2021-01-01", "2021-01-01"),   # first night inclusive
    ("2024-02-10", "2024-02-10"),   # ongoing first night
    ("2023-09-01", "2023-09-01"),
])
def test_full_coverage_has_no_flag_and_no_spans(a, b):
    r = read(ok(), a, b)
    assert r.flags == () and r.uncovered == ()


def test_trailing_window_edge_after_last_ended_period():
    only = [_period(effective_from="2021-01-01", effective_to="2021-06-01", end_status="ended")]
    r = read(ok(only), "2021-05-30", "2021-06-10")
    assert r.flags == (FLAG,) and r.uncovered == ((d("2021-06-01"), d("2021-06-10")),)


def test_unsorted_response_is_sorted_and_overlap_does_not_double_count():
    ps = [_period(effective_from="2024-01-01"), _period(effective_from="2021-01-01", effective_to="2024-01-01", end_status="ended")]
    r = read(ok(ps), "2023-12-31", "2024-01-02")
    assert [p.effective_from for p in r.periods] == [d("2021-01-01"), d("2024-01-01")] and r.flags == ()


def test_uncovered_is_flagged_not_refused_vs_wholly_unknown_refused():
    assert read(ok(), "2023-06-01", "2023-07-01").flags == (FLAG,)
    assert refusal(ok(), "2023-07-01", "2023-07-02").code == "NO_HOME_RECORDED"


def test_fixture_has_every_state_and_a_gap_and_no_address():
    ps = FIXTURE["result"]["periods"]
    assert {p["end_status"] for p in ps} == {"ended", "ongoing", "end_unknown"}
    assert all(p["effective_to"] is None for p in ps if p["end_status"] != "ended")
    assert FIXTURE["result"]["gaps"]
    for banned in ("street", "label", "postal_code", "unit", "notes", "source_doc"):
        assert all(banned not in p for p in ps)


def test_it_stays_out_of_the_lane_registry():
    assert "home_service" not in c.LANE_MODULES.values() and not hasattr(hs, "HANDLERS")
