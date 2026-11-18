"""A-8 (FLY-TSK-029-0C, FLY-TSK-078): the direct call to finance and its three failure behaviours, against a fixture.

Offline: the Lambda layer is a stub; no network, no AWS, no real finance call. home_service imports only
contract and settings (stdlib otherwise), so these run without io_kit_mcp_edge.
"""
import inspect
import json
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import contract as c  # noqa: E402
import home_service as hs  # noqa: E402
import settings as st  # noqa: E402

FIXTURE = json.loads((ROOT / "tests" / "fixtures" / "finance_residence_periods_fixture.json").read_text())
PINNED = D / "contracts" / "residence_read_v1.json"
CONTRACT = json.loads(PINNED.read_text())
FN = "finance-fn-test"


class Stub:
    """The invoker: records (function, event, timeout) and returns (function_error, reply bytes)."""

    def __init__(self, body=b"", function_error=None, exc=None, seq=None):
        self.calls, self.body, self.function_error, self.exc, self.seq = [], body, function_error, exc, seq

    def __call__(self, function, payload, timeout):
        self.calls.append((function, json.loads(payload), timeout))
        if self.seq:
            return self.seq.pop(0)
        if self.exc:
            raise self.exc
        return self.function_error, self.body


def reply(periods):
    return json.dumps({"ok": True, "result": {"periods": periods}}).encode()


def ok(periods=None):
    return Stub(reply(FIXTURE["result"]["periods"] if periods is None else periods))


def read(stub, a, b):
    return hs.read_home(a, b, invoker=stub, function=FN)


def refusal(stub, a="2021-02-01", b="2021-03-01"):
    with pytest.raises(hs.HomeError) as e:
        read(stub, a, b)
    return e.value


# ---------------------------------------------------------------- FLY-TSK-078: the pinned contract

def test_pinned_fixture_is_byte_identical_to_finances():
    """The fixture is finance's file, copied byte for byte; its sha256 is pinned so an edit here is loud."""
    import hashlib
    assert hashlib.sha256(PINNED.read_bytes()).hexdigest() == "74f036fb3963d195d9a9f6cb04319ecdd754bc047011e2f71342df60698664d8"


def test_request_builder_produces_the_fixtures_event():
    ev = CONTRACT["event"]
    assert hs._event(ev["args"]["from"], ev["args"]["to"]) == ev
    s = Stub(json.dumps(CONTRACT["reply"]).encode())
    read(s, ev["args"]["from"], ev["args"]["to"])
    assert s.calls[0][1] == ev and "requestContext" not in s.calls[0][1]


def test_fixture_reply_parses():
    r = read(Stub(json.dumps(CONTRACT["reply"]).encode()), "2000-06-01", "2000-06-30")
    assert len(r.periods) == 1 and r.periods[0].residence_uid == "resd_sample" and r.periods[0].end_status == "ongoing"
    assert r.flags == ()


def test_fixture_error_reply_is_unavailable_and_an_alert():
    e = refusal(Stub(json.dumps(CONTRACT["error_reply"]).encode()))
    assert e.code == "HOME_UNAVAILABLE" and e.cause == "invalid_args" and e.alert


def test_invoke_goes_through_boto_with_request_response(monkeypatch):
    seen = {}

    class Body:
        def read(self):
            return reply([])

    class Client:
        def invoke(self, **kw):
            seen.update(kw)
            return {"StatusCode": 200, "Payload": Body()}

    monkeypatch.setattr(hs, "_lambda_client", Client())
    assert hs._boto_invoker(FN, b"{}", 5.0) == (None, reply([]))
    assert seen == {"FunctionName": FN, "InvocationType": "RequestResponse", "Payload": b"{}"}
    assert not hasattr(hs, "urllib") and "urllib" not in (D / "home_service.py").read_text()


@pytest.mark.parametrize("value,good", [
    ("finance-mcp", True), ("arn:aws:lambda:us-west-2:123456789012:function:finance-mcp", True),
    ("arn:aws:lambda:us-west-2:123456789012:function:finance-mcp:live", True),
    ("https://abc.lambda-url.us-west-2.on.aws/", False), ("http://x", False), ("", False), ("a b", False),
    ("arn:aws:s3:::bucket", False), ("finance/mcp", False), ("x" * 65, False),
])
def test_finance_read_function_must_be_a_function_name_or_arn(monkeypatch, value, good):
    assert (st.finance_function_problem(value) == "") is good
    monkeypatch.setenv("FINANCE_READ_FUNCTION", value)
    if good:
        s = ok()
        hs.read_home("2021-02-01", "2021-03-01", invoker=s)
        assert s.calls[0][0] == value
    else:
        s = ok()
        with pytest.raises(hs.HomeError) as e:
            hs.read_home("2021-02-01", "2021-03-01", invoker=s)
        assert e.value.cause == "config" and not s.calls
        assert not value or value not in str(e.value)


def test_bearer_and_secret_path_are_gone():
    src = (D / "home_service.py").read_text().lower()
    for gone in ("bearer", "secretsmanager", "authorization", "get_secret_value", "x-agent-session", "https://"):
        assert gone not in src, gone
    assert "FINANCE_READ_CREDENTIAL_SECRET" not in c.SETTINGS and not hasattr(hs, "SECRET_SETTING")
    assert c.SETTINGS["FINANCE_READ_FUNCTION"]["required"] == "public"  # required-ness unchanged


def test_the_deploy_strips_the_retired_secret_setting_and_refuses_a_url():
    import resolve_env
    live = {"FINANCE_READ_CREDENTIAL": "x", "FINANCE_READ_CREDENTIAL_SECRET": "some/secret/name", "TABLE_NAME": "t"}
    out = resolve_env.resolve(live, {})
    assert "FINANCE_READ_CREDENTIAL" not in out and "FINANCE_READ_CREDENTIAL_SECRET" not in out
    assert "function name or ARN" in resolve_env.guard({"FINANCE_READ_FUNCTION": "https://x.example/"})


# ---------------------------------------------------------------- clause 1: the trips read sends no home

def test_signature_takes_no_home_and_no_tool():
    params = inspect.signature(hs.read_home).parameters
    assert [n for n, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD] == ["date_from", "date_to"]
    assert not [n for n in params if any(w in n.lower() for w in ("home", "residence", "period", "place", "tool", "name", "method"))]
    for n in ("invoker", "function"):
        assert params[n].kind is inspect.Parameter.KEYWORD_ONLY
    with pytest.raises(TypeError):
        hs.read_home("2021-02-01", "2021-03-01", home="res-a", invoker=ok(), function=FN)


def test_request_carries_only_the_window_and_the_fixed_event():
    s = ok()
    read(s, "2021-02-01", "2021-03-01")
    function, event, timeout = s.calls[0]
    assert function == FN and timeout == hs.TIMEOUT_SECONDS
    assert event == {"internal": "finance.residence_periods.v1", "args": {"from": "2021-02-01", "to": "2021-03-01"}}


# ---------------------------------------------------------------- clause 5: read scoping

def test_only_the_residence_read_can_be_sent():
    assert hs.INTERNAL_CONTRACT == "finance.residence_periods.v1"
    assert inspect.signature(hs._event).parameters.keys() == {"date_from", "date_to"}
    src = (D / "home_service.py").read_text()
    for forbidden in ("finance_query", "finance.query", "record_residence", "close_residence", "create_residence"):
        assert forbidden not in src
    for a, b in [("2021-02-01", "2021-02-02"), ("2024-03-01", "2024-03-09")]:
        s = ok()
        read(s, a, b)
        assert s.calls[0][1]["internal"] == hs.INTERNAL_CONTRACT
        assert set(s.calls[0][1]["args"]) == {"from", "to"} and set(s.calls[0][1]) == {"internal", "args"}
    public = [n for n, o in vars(hs).items() if inspect.isfunction(o) and not n.startswith("_") and o.__module__ == "home_service"]
    assert public == ["read_home"]


def test_projection_drops_everything_but_the_narrow_fields():
    dirty = [{"effective_from": "2024-01-01", "effective_to": None, "end_status": "ongoing", "residence_uid": "res-d",
              "tenure": "owned", "state": "S4", "country": "ZZ", "label": "PLACEHOLDER-LABEL", "street": "PLACEHOLDER-STREET",
              "notes": "n", "postal_code": "00000"}]
    r = read(ok(dirty), "2024-02-01", "2024-02-05")
    assert {f for f in vars(r.periods[0])} == {"effective_from", "effective_to", "end_status", "residence_uid", "tenure", "state", "country"}
    assert "PLACEHOLDER" not in repr(r)


def test_function_name_is_never_in_errors():
    for stub in (Stub(exc=OSError(f"connect {FN}")), Stub(function_error="Unhandled"), ok([])):
        e = refusal(stub)
        assert FN not in str(e) and FN not in repr(e) and FN not in e.cause


def test_function_defaults_to_declared_setting(monkeypatch):
    monkeypatch.setenv(hs.FUNCTION_SETTING, "finance-from-env")
    s = ok()
    hs.read_home("2021-02-01", "2021-03-01", invoker=s)
    assert s.calls[0][0] == "finance-from-env"
    monkeypatch.delenv(hs.FUNCTION_SETTING)
    with pytest.raises(hs.HomeError) as e:
        hs.read_home("2021-02-01", "2021-03-01", invoker=ok())
    assert e.value.code == "HOME_UNAVAILABLE" and e.value.cause == "config"


# ---------------------------------------------------------------- clause 2: finance unreachable -> HOME_UNAVAILABLE
# The finding's s6 rows 1-10 and 14.

def reply_err(code):
    return Stub(json.dumps({"ok": False, "error": {"code": code, "message": "m"}}).encode())


UNAVAILABLE = {
    # FunctionError or an ok:false reply
    "function_error": lambda: Stub(b'{"errorMessage":"boom"}', function_error="Unhandled"),
    "function_error_handled": lambda: Stub(b"{}", function_error="Handled"),
    "ok_false_invalid_args": lambda: reply_err("invalid_args"),
    "ok_false_other": lambda: reply_err("backend_error"),
    "ok_false_new_code": lambda: reply_err("something_new"),
    "ok_false_no_error": lambda: Stub(b'{"ok": false}'),
    # timeout or unavailable
    "transport_oserror": lambda: Stub(exc=OSError("dns")),
    "timeout": lambda: Stub(exc=TimeoutError("t")),
    "throttled": lambda: Stub(exc=RuntimeError("TooManyRequestsException")),
    "access_denied": lambda: Stub(exc=RuntimeError("AccessDeniedException")),
    "function_missing": lambda: Stub(exc=RuntimeError("ResourceNotFoundException")),
    # malformed reply
    "not_json": lambda: Stub(b"<html>"),
    "empty": lambda: Stub(b""),
    "json_not_object": lambda: Stub(b"[1]"),
    "ok_missing": lambda: Stub(b'{"result": {"periods": []}}'),
    "ok_not_bool": lambda: Stub(b'{"ok": "yes", "result": {"periods": []}}'),
    "result_missing": lambda: Stub(b'{"ok": true}'),
    "result_not_object": lambda: Stub(b'{"ok": true, "result": []}'),
    "periods_not_list": lambda: Stub(b'{"ok": true, "result": {"periods": {}}}'),
    "periods_missing": lambda: Stub(b'{"ok": true, "result": {}}'),
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


def test_alerting_tags_distinguish_bugs_and_config_from_outages():
    assert refusal(reply_err("invalid_args")).alert
    assert not refusal(Stub(exc=TimeoutError("t"))).alert and not refusal(reply_err("backend_error")).alert
    assert not refusal(Stub(b"x", function_error="Unhandled")).alert


def test_one_invoke_per_read_no_retry():
    s = Stub(exc=TimeoutError("t"))
    refusal(s)
    assert len(s.calls) == 1


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
