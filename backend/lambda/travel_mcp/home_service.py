"""Lane 2C (FLY-TSK-029-0C, FLY-TSK-078): travel's one call to finance for residence periods.

Contract: DOC-4CEAAE4B8CB7 (lane 0C); the call itself is the direct-invoke contract pinned by
contracts/residence_read_v1.json (byte-identical to finance's copy). Not a tool module: it is not in
contract.LANE_MODULES and exposes no HANDLERS. Lanes 3A and 3B import it; lane 2D's engine consumes its HomeRead.

ENTRY POINT (sync; run it with asyncio.to_thread from an async handler):

    read_home(date_from: str, date_to: str, *, invoker=None, function=None) -> HomeRead

date_from and date_to are the trip window, ISO dates, BOTH INCLUSIVE. There is no home argument:
the caller never supplies home, so the trips read cannot send one (objective O-4, criterion A-8).
The keyword-only parameters are dependency injection for the Lambda layer and the configuration, never
data about home and never a tool choice.

TRANSPORT (FLY-TSK-078): a direct lambda:Invoke of the function named by FINANCE_READ_FUNCTION, through
this function's own execution role. No HTTP hop and no stored credential. The event is
{"internal": "finance.residence_periods.v1", "args": {"from", "to"}} with no requestContext; the reply is
{"ok": true, "result": {"periods": [...]}} or {"ok": false, "error": {"code", "message"}}.

Outcomes
  * HomeRead(periods, uncovered, flags): some of the window is covered. When part is not, `uncovered`
    lists the inclusive (from, to) spans and `flags` carries contract.FLAGS "home not recorded for
    these dates". The trip is flagged, not refused.
  * HomeError("NO_HOME_RECORDED"): no part of the window is covered (empty list, every period outside
    the window, or only an end_unknown period whose single known night misses the window).
  * HomeError("HOME_UNAVAILABLE"): finance could not be asked or answered unusably (see _unwrap):
    a FunctionError or an ok:false reply, a timeout or unavailable function, or a malformed reply.

Read scoping (A-8, last clause). The event names one fixed internal contract, the request builder takes no
name, arguments are only from/to, and HomePeriod keeps only the dates, end_status, tenure, state and
country. Nothing else a response carries is retained.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable, Optional

import contract
import settings

INTERNAL_CONTRACT = "finance.residence_periods.v1"  # the ONLY read this module can send
TIMEOUT_SECONDS = 10.0
FUNCTION_SETTING = "FINANCE_READ_FUNCTION"  # declared in contract.SETTINGS; a Lambda function name or ARN
END_STATUSES = ("ended", "ongoing", "end_unknown")
NOT_RECORDED_FLAG = "home not recorded for these dates"
assert NOT_RECORDED_FLAG in contract.FLAGS

# Why an ok:false reply is HOME_UNAVAILABLE, keyed by finance's error code.
_REPLY_ERRORS = {
    "invalid_args": "finance rejected the request arguments (travel bug)",
}


class HomeError(Exception):
    """Carries one of contract.ERROR_CODES: HOME_UNAVAILABLE or NO_HOME_RECORDED."""

    def __init__(self, code: str, message: str, *, cause: str = ""):
        assert code in ("HOME_UNAVAILABLE", "NO_HOME_RECORDED")
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = contract.ERROR_CODES[code]
        self.cause = cause  # short machine tag for alerting; never a credential or a response body
        self.alert = cause in ("invalid_args", "contract_violation", "config")


@dataclass(frozen=True)
class HomePeriod:
    """The narrow projection. No street, unit, label, notes or document reference exists here."""
    effective_from: date
    effective_to: Optional[date]  # exclusive end; None only for ongoing / end_unknown
    end_status: str
    residence_uid: str
    tenure: str
    state: Optional[str]
    country: Optional[str]


@dataclass(frozen=True)
class HomeRead:
    window: tuple  # (date_from, date_to) inclusive, as asked
    periods: tuple
    uncovered: tuple = ()  # inclusive (from, to) date spans of the window with no known home
    flags: tuple = ()

    def period_for(self, day: date) -> Optional[HomePeriod]:
        """The period known to cover `day`; end_unknown covers only its first night."""
        for p in self.periods:
            if p.effective_from <= day < _known_end(p):
                return p
        return None


# --------------------------------------------------------------------------- configuration

def _function_name() -> str:
    value = os.environ.get(FUNCTION_SETTING, "").strip()
    problem = settings.finance_function_problem(value)
    if problem:
        raise HomeError("HOME_UNAVAILABLE", f"{FUNCTION_SETTING} {problem}", cause="config")
    return value


# --------------------------------------------------------------------------- transport

_lambda_client = None


def _boto_invoker(function: str, payload: bytes, timeout: float) -> tuple:
    """One RequestResponse invoke; returns (function_error or None, reply bytes). Raises on transport failure."""
    global _lambda_client
    if _lambda_client is None:
        import boto3  # lazy: tests inject an invoker and need no AWS
        from botocore.config import Config
        _lambda_client = boto3.client("lambda", config=Config(
            connect_timeout=3, read_timeout=timeout, retries={"max_attempts": 1}))
    resp = _lambda_client.invoke(FunctionName=function, InvocationType="RequestResponse", Payload=payload)
    return resp.get("FunctionError") or None, resp["Payload"].read()


def _event(date_from: str, date_to: str) -> dict:
    return {"internal": INTERNAL_CONTRACT, "args": {"from": date_from, "to": date_to}}


# --------------------------------------------------------------------------- parsing

def _violation(what: str) -> HomeError:
    return HomeError("HOME_UNAVAILABLE", f"finance response violates the contract: {what}", cause="contract_violation")


def _iso(value, what: str) -> date:
    if not isinstance(value, str) or len(value) != 10:
        raise _violation(f"{what} is not an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise _violation(f"{what} is not an ISO date") from None


def _parse_period(raw) -> HomePeriod:
    if not isinstance(raw, dict):
        raise _violation("a period is not an object")
    start = _iso(raw.get("effective_from"), "effective_from")
    status = raw.get("end_status")
    if status not in END_STATUSES:
        raise _violation("end_status is not one of ended, ongoing, end_unknown")
    end_raw = raw.get("effective_to")
    if status == "ended":
        end = _iso(end_raw, "effective_to")
        if end <= start:
            raise _violation("an ended period ends on or before it starts")
    else:
        if end_raw is not None:
            raise _violation(f"a {status} period carries an end")
        end = None
    uid, tenure = raw.get("residence_uid"), raw.get("tenure")
    if not isinstance(uid, str) or not uid or not isinstance(tenure, str):
        raise _violation("residence_uid or tenure missing")
    state, country = raw.get("state"), raw.get("country")
    for v in (state, country):
        if v is not None and not isinstance(v, str):
            raise _violation("state or country is not a string")
    return HomePeriod(start, end, status, uid, tenure, state, country)


def _unwrap(function_error, body: bytes) -> dict:
    """Map invoke outcomes to the reply's result, or HOME_UNAVAILABLE."""
    if function_error:
        raise HomeError("HOME_UNAVAILABLE", "finance function failed (FunctionError)", cause="backend_error")
    try:
        reply = json.loads(body)
        if not isinstance(reply, dict):
            raise ValueError
    except (TypeError, ValueError):
        raise _violation("reply is not a JSON object") from None
    if reply.get("ok") is False:
        err = reply.get("error")
        code = err.get("code") if isinstance(err, dict) else None
        cause = "invalid_args" if code == "invalid_args" else "backend_error"
        raise HomeError("HOME_UNAVAILABLE", _REPLY_ERRORS.get(code, "finance reported an error"), cause=cause)
    if reply.get("ok") is not True or not isinstance(reply.get("result"), dict):
        raise _violation("ok is not true or result is missing")
    return reply["result"]


# --------------------------------------------------------------------------- coverage

def _known_end(p: HomePeriod) -> date:
    """First day NOT known to be home. ongoing is open-ended; end_unknown is its first night only."""
    if p.end_status == "ended":
        return p.effective_to
    if p.end_status == "end_unknown":
        return p.effective_from + timedelta(days=1)
    return date.max


def _uncovered(periods, w_from: date, w_to: date) -> list:
    """Inclusive spans of [w_from, w_to] not covered by any period's known interval (leading, internal
    and trailing gaps). A null end is resolved by _known_end BEFORE any gap arithmetic."""
    spans, cursor, limit = [], w_from, w_to + timedelta(days=1)
    for p in sorted(periods, key=lambda p: p.effective_from):
        start, end = p.effective_from, _known_end(p)
        if end <= cursor:
            continue
        if start >= limit:
            break
        if start > cursor:
            spans.append((cursor, start - timedelta(days=1)))
        cursor = max(cursor, end)
        if cursor >= limit:
            return spans
    if cursor < limit:
        spans.append((cursor, w_to))
    return spans


# --------------------------------------------------------------------------- entry point

def read_home(date_from: str, date_to: str, *, invoker: Optional[Callable] = None,
              function: Optional[str] = None) -> HomeRead:
    w_from = date.fromisoformat(date_from) if isinstance(date_from, str) and len(date_from) == 10 else None
    w_to = date.fromisoformat(date_to) if isinstance(date_to, str) and len(date_to) == 10 else None
    if w_from is None or w_to is None or w_to < w_from:
        raise ValueError("date_from and date_to must be ISO dates with date_from <= date_to")

    target = function if function is not None else _function_name()
    payload = json.dumps(_event(date_from, date_to)).encode()
    send = invoker or _boto_invoker
    try:
        function_error, raw = send(target, payload, TIMEOUT_SECONDS)
    except Exception:  # timeout, throttle, unavailable, access: the text can name the function; not kept
        raise HomeError("HOME_UNAVAILABLE", "finance is unreachable", cause="unreachable") from None
    result = _unwrap(function_error, raw)

    plist = result.get("periods")
    if not isinstance(plist, list):
        raise _violation("periods is not a list")
    periods = tuple(sorted((_parse_period(p) for p in plist), key=lambda p: p.effective_from))
    gaps = _uncovered(periods, w_from, w_to)
    if gaps == [(w_from, w_to)]:
        reason = "no residence period recorded" if not periods else "no recorded residence period covers these dates"
        raise HomeError("NO_HOME_RECORDED", reason, cause="none_covering")
    flags = (NOT_RECORDED_FLAG,) if gaps else ()
    return HomeRead((w_from, w_to), periods, tuple(gaps), flags)
