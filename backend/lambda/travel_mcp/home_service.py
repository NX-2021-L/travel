"""Lane 2C (FLY-TSK-029-0C): travel's one call to finance for residence periods.

Contract: DOC-4CEAAE4B8CB7 (lane 0C). Not a tool module: it is not in contract.LANE_MODULES and
exposes no HANDLERS. Lanes 3A and 3B import it; lane 2D's engine consumes its HomeRead.

ENTRY POINT (sync; run it with asyncio.to_thread from an async handler):

    read_home(date_from: str, date_to: str, *, transport=None, credential=None, endpoint=None) -> HomeRead

date_from and date_to are the trip window, ISO dates, BOTH INCLUSIVE. There is no home argument:
the caller never supplies home, so the trips read cannot send one (objective O-4, criterion A-8).
The keyword-only parameters are dependency injection for the HTTP layer and the configuration, never
data about home and never a tool choice.

Outcomes
  * HomeRead(periods, uncovered, flags): some of the window is covered. When part is not, `uncovered`
    lists the inclusive (from, to) spans and `flags` carries contract.FLAGS "home not recorded for
    these dates". The trip is flagged, not refused.
  * HomeError("NO_HOME_RECORDED"): no part of the window is covered (empty list, every period outside
    the window, or only an end_unknown period whose single known night misses the window).
  * HomeError("HOME_UNAVAILABLE"): finance could not be asked or answered unusably (see _unwrap).

Read scoping (A-8, last clause). Finance's agent key is full-access and finance has no read scoping
below "authenticated and allow-listed", so the guarantee is enforced HERE: RESIDENCE_TOOL is a module
constant, the request builder takes no tool name, arguments are only from/to, and HomePeriod keeps
only the dates, end_status, tenure, state and country. Nothing else a response carries is retained.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable, Optional

import contract

RESIDENCE_TOOL = "finance_residence_periods"  # the ONLY tool this module can send
SESSION_NAME = "io-travel"
TIMEOUT_SECONDS = 10.0
ENDPOINT_SETTING = "FINANCE_READ_FUNCTION"  # declared in contract.SETTINGS; its value must be the Function URL
END_STATUSES = ("ended", "ongoing", "end_unknown")
NOT_RECORDED_FLAG = "home not recorded for these dates"
assert NOT_RECORDED_FLAG in contract.FLAGS

# Why a tool-level failure is HOME_UNAVAILABLE, keyed by finance's error code (finding s6 rows 7-10).
_TOOL_ERRORS = {
    "unknown_tool": "finance does not serve the residence-period tool (not deployed or misnamed)",
    "invalid_input": "finance rejected the request arguments (travel bug)",
    "storage_conflict": "finance storage conflict",
    "backend_error": "finance internal failure",
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
        self.alert = cause in ("bad_request", "invalid_input", "contract_violation", "config")


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

def _env_endpoint() -> str:
    return os.environ.get(ENDPOINT_SETTING, "").strip()


def _missing_credential() -> str:
    """No setting for finance's credential is declared in contract.SETTINGS, so none is read.
    Until one is declared, a deployment cannot reach finance and every read is HOME_UNAVAILABLE."""
    raise HomeError("HOME_UNAVAILABLE", "finance credential is not configured (no setting declared)", cause="config")


# --------------------------------------------------------------------------- transport

def _urllib_transport(url: str, headers: dict, body: bytes, timeout: float) -> tuple:
    """POST; returns (status, body bytes). Raises OSError-family on transport failure."""
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - https enforced by caller
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() or b""


def _request_body(date_from: str, date_to: str) -> bytes:
    return json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": RESIDENCE_TOOL, "arguments": {"from": date_from, "to": date_to}},
    }).encode()


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


def _unwrap(status: int, body: bytes) -> dict:
    """Map transport and envelope outcomes (finding s6 rows 1-10) to the tool result, or HOME_UNAVAILABLE."""
    if status in (400, 405):
        raise HomeError("HOME_UNAVAILABLE", f"finance refused the request shape (HTTP {status}; travel bug)", cause="bad_request")
    if status in (401, 403):
        raise HomeError("HOME_UNAVAILABLE", f"finance refused travel's credential (HTTP {status}; configuration fault)", cause="config")
    if status == 429:
        raise HomeError("HOME_UNAVAILABLE", "finance is throttling (HTTP 429)", cause="unreachable")
    if not 200 <= status < 300:
        raise HomeError("HOME_UNAVAILABLE", f"finance answered HTTP {status}", cause="unreachable")
    try:
        rpc = json.loads(body)
        if not isinstance(rpc, dict):
            raise ValueError
    except ValueError:
        raise _violation("body is not a JSON-RPC object") from None
    if "error" in rpc:  # JSON-RPC level error, e.g. -32603
        raise HomeError("HOME_UNAVAILABLE", "finance returned a JSON-RPC error", cause="backend_error")
    result = rpc.get("result")
    try:
        inner = json.loads(result["content"][0]["text"])
        if not isinstance(inner, dict):
            raise ValueError
    except (TypeError, KeyError, IndexError, ValueError):
        raise _violation("tool result envelope is malformed") from None
    if result.get("isError") or inner.get("success") is False:
        err = inner.get("error")
        code = err.get("code") if isinstance(err, dict) else None
        cause = code if code in _TOOL_ERRORS else "backend_error"
        raise HomeError("HOME_UNAVAILABLE", _TOOL_ERRORS.get(code, "finance reported an unrecognised tool error"), cause=cause)
    if inner.get("success") is not True or not isinstance(inner.get("result"), dict):
        raise _violation("success is not true or result is missing")
    return inner["result"]


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

def read_home(date_from: str, date_to: str, *, transport: Optional[Callable] = None,
              credential: Optional[Callable[[], str]] = None, endpoint: Optional[str] = None) -> HomeRead:
    w_from = date.fromisoformat(date_from) if isinstance(date_from, str) and len(date_from) == 10 else None
    w_to = date.fromisoformat(date_to) if isinstance(date_to, str) and len(date_to) == 10 else None
    if w_from is None or w_to is None or w_to < w_from:
        raise ValueError("date_from and date_to must be ISO dates with date_from <= date_to")

    url = endpoint if endpoint is not None else _env_endpoint()
    if not url.startswith("https://"):
        raise HomeError("HOME_UNAVAILABLE", f"{ENDPOINT_SETTING} is not set to finance's https Function URL", cause="config")
    token = (credential or _missing_credential)()
    if not token:
        raise HomeError("HOME_UNAVAILABLE", "finance credential is empty", cause="config")
    send = transport or _urllib_transport
    headers = {"Authorization": f"Bearer {token}", "X-Agent-Session": SESSION_NAME, "Content-Type": "application/json"}
    body = _request_body(date_from, date_to)

    result = None
    for attempt in (1, 2):
        try:
            status, raw = send(url, headers, body, TIMEOUT_SECONDS)
        except Exception:  # transport: DNS, refused, timeout, TLS. The exception text is not kept.
            raise HomeError("HOME_UNAVAILABLE", "finance is unreachable", cause="unreachable") from None
        try:
            result = _unwrap(status, raw)
            break
        except HomeError as e:
            if e.cause == "storage_conflict" and attempt == 1:
                continue  # safe to retry once (row 9)
            raise

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
