"""The write half of the records door (FLY-TSK-087, BRD DOC-D14184D324F0 6.7): trip corrections only.

  POST /records/recordsets/trip_corrections/intents     create | retire | undo   (Idempotency-Key header or body key)
  GET  /records/recordsets/{id}/writes                  the corrections' create and retire events as WriteEntry

Every write goes through the two tool handlers and nothing else (T-28):
  create -> lane_2e_corrections.record_trip_correction      author = the human-door subject
  retire -> lane_2e_corrections.retire_trip_correction      if_version must equal the row's version
  undo   -> retire_trip_correction, for a create the caller made within the undo window (undo of a create is a retire)
The handlers validate the fields each kind needs and name the field at fault; this module turns that into a field
problem. A retire offers no undo: the corrections lane has no handler that restores one (undo_closed no_inverse).
Trips stay computed on read, so a receipt names both sets in `refreshed`.

Doors: "human" for a write through this door; "agent" for a write through the MCP tools (they carry no door
attribute). Travel has no scheduled or internal writer, so it never serves "service", and nothing is settled by
evidence, so it never serves "settled".
"""
import asyncio
import base64
import json
from datetime import datetime, timedelta, timezone

import lane_2e_corrections as corrections

SET = "trip_corrections"
REFRESHED = [SET, "trips"]
UNDO_WINDOW = timedelta(seconds=300)
# Canvas A7.7 (FLY-TSK-092): the panel never asks for method, supersedes or question_id. A confirmation made through
# this door is "examined"; supersedes and question_id belong to the question flow (the MCP tools), not this door.
CREATE_FIELDS = ("kind", "date", "record_ids", "first_date", "last_date")
DOOR_CONFIRMATION_METHOD = "examined"
_STATUS = {"invalid": 422, "missing": 404, "not_allowed": 403, "duplicate": 422}
# WriteEntry.principal names who wrote in words, never an identifier (io, 2026-11-25: no id on any DVP-PLN-021
# screen; finance 2X serves the same words). It is taken from the door: a person through the records door is "you",
# an MCP tool call is "agent". Travel has no scheduled writer, so "scheduler" is never served (kept for parity).
PRINCIPAL_WORDS = {"human": "you", "agent": "agent", "service": "scheduler"}


def principal_word(door):
    return PRINCIPAL_WORDS.get(door, "agent")


class Refusal(Exception):
    """A refusal that writes nothing: an HTTP status, a problem, and for a conflict the current rows."""

    def __init__(self, status, title, detail, field=None, rows=None, conflict=False, problem=False):
        """problem=True answers a problem document (the intent itself is refused: its kind, its key, its token);
        otherwise a rejected or conflict receipt (a value of the intent is refused)."""
        super().__init__(detail)
        self.status, self.title, self.detail, self.field, self.rows, self.conflict = status, title, detail, field, rows, conflict
        self.problem = problem


# --------------------------------------------------------------------------- helpers
def _parse_time(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def _iso(t):
    return t.isoformat(timespec="seconds").replace("+00:00", "Z")


def _now():
    return datetime.now(timezone.utc)


def undo_token(cid, key):
    return base64.urlsafe_b64encode(json.dumps({"s": SET, "c": cid, "k": key}, separators=(",", ":")).encode()).decode().rstrip("=")


def _read_token(token):
    try:
        raw = json.loads(base64.urlsafe_b64decode(str(token) + "=" * (-len(str(token)) % 4)))
        if raw["s"] != SET or not isinstance(raw["c"], str) or not isinstance(raw["k"], str):
            raise ValueError
        return raw["c"], raw["k"]
    except Exception:  # noqa: BLE001 - every unreadable token reads the same
        raise Refusal(422, "Undo not available", "The undo token is not one this set issued.", "undo_token", problem=True)


def _undo_for(c, audit, caller):
    """(undo, undo_closed) for a create: live for its author through this door within the window; else closed."""
    if c["status"] != "active":
        return None, "changed"
    if not audit.get("idempotency_key"):
        return None, None  # an MCP write: no records-door token was ever issued for it
    expires = _parse_time(c["created_at"]) + UNDO_WINDOW
    if _now() >= expires:
        return None, "expired"
    if c["author"] != caller:
        return None, None
    return {"token": undo_token(c["correction_id"], audit["idempotency_key"]), "expires_at": _iso(expires)}, None


def _call(handler, args, caller, **ctx):
    """One handler call as the human-door subject: the handler stamps the author from the request principal."""
    lf = corrections._lf()
    held = lf._PRINCIPAL.set({"kind": "records-door", "id": caller, "sub": caller, "groups": [], "allowed": True})
    try:
        with corrections.door_write(**ctx):
            out = json.loads(asyncio.run(handler(args))[0].text)
    finally:
        lf._PRINCIPAL.reset(held)
    if out.get("success"):
        return out["result"]
    env = out.get("error_envelope") or {}
    code = env.get("code")
    if code in _STATUS:
        raise Refusal(_STATUS[code], "Write refused", env.get("message") or out.get("error"), env.get("field"))
    raise Refusal(503, "Records unavailable", env.get("message") or "The write could not be made.")


def _current(routes, cid):
    return next((c for c in corrections.load_corrections(None, include_retired=True) if c["correction_id"] == cid), None)


def _receipt(routes, rows, undo=None):
    _, _, vintage = routes._correction_rows({})
    return {"status": "applied", "rows": rows, "problems": [], "undo": undo, "vintage": vintage,
            "refreshed": list(REFRESHED), "stale": None}


# --------------------------------------------------------------------------- intents
def _create(routes, intent, caller, key):
    held = corrections.correction_for_intent_key(key)
    if held is not None and not _same_create(held, intent, caller):
        raise Refusal(422, "Idempotency key reused", "This key was used for another intent.", "idempotency_key", problem=True)
    patch = intent.get("patch")
    if not isinstance(patch, dict):
        raise Refusal(422, "Write refused", "A create carries the new correction's fields in patch.", "patch")
    extra = sorted(set(patch) - set(CREATE_FIELDS))
    if extra:
        raise Refusal(422, "Write refused", f"{extra[0]} is not a field a correction is created with.", extra[0])
    args = {**patch, "reason": intent["reason"]}
    if patch.get("kind") == "confirmed":
        args["method"] = DOOR_CONFIRMATION_METHOD
    c = _call(corrections.record_trip_correction, args, caller, door="human", idempotency_key=key)
    undo, _ = _undo_for(c, {"idempotency_key": key}, caller)
    return _receipt(routes, [routes._correction_row(c)], undo)


def _same_create(held, intent, caller):
    """Whether a correction made under this key is the one this create intent would make (a replay)."""
    patch = intent.get("patch") if isinstance(intent.get("patch"), dict) else {}
    ids = patch.get("record_ids")
    if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids):
        return False  # a malformed list is never a replay; the create refuses it at field record_ids (DVP-ISS-180)
    want = {"kind": patch.get("kind"), "date": patch.get("date"), "record_ids": sorted({i.strip() for i in ids}),
            "reason": intent.get("reason"), "author": caller}
    have = {"kind": held.get("kind"), "date": held.get("date"), "record_ids": sorted(held.get("record_ids") or []),
            "reason": held.get("reason"), "author": held.get("author")}
    extra = {k: patch.get(k) for k in ("first_date", "last_date") if patch.get(k) is not None}
    return want == have and all(held.get(k) == v for k, v in extra.items()) and not (set(patch) - set(CREATE_FIELDS))


def _retire(routes, intent, caller, key):
    cid = intent.get("key")
    if not isinstance(cid, str) or not cid:
        raise Refusal(422, "Write refused", "A retire names the correction in key.", "key")
    if intent.get("if_version") is None:
        raise Refusal(422, "Write refused", "A retire carries the row's version in if_version.", "if_version")
    c = _current(routes, cid)
    if c is None:
        raise Refusal(404, "Not found", "No such correction.", "key", problem=True)
    audit = corrections.load_correction_audit().get(cid, {})
    if c["status"] == "retired" and audit.get("retire_idempotency_key") == key:
        return _receipt(routes, [routes._correction_row(c)])  # a replay: the same end state, written once
    row = routes._correction_row(c)
    if str(row["version"]) != str(intent["if_version"]):
        raise Refusal(409, "Conflict", "The correction changed since it was read.", rows=[row], conflict=True)
    done = _call(corrections.retire_trip_correction, {"correction_id": cid, "reason": intent["reason"]}, caller,
                 door="human", idempotency_key=key, via="retire")
    return _receipt(routes, [routes._correction_row(done)])


def _undo(routes, intent, caller, key):
    cid, create_key = _read_token(intent.get("undo_token"))
    c = _current(routes, cid)
    audit = corrections.load_correction_audit().get(cid, {})
    if c is None or audit.get("idempotency_key") != create_key or c["author"] != caller:
        raise Refusal(422, "Undo not available", "The undo token is not one this set issued to this caller.", "undo_token",
                      problem=True)
    if c["status"] == "retired":
        if audit.get("retire_idempotency_key") == key:
            return _receipt(routes, [routes._correction_row(c)])  # a replay
        raise Refusal(409, "Conflict", "The correction was retired since; there is nothing to undo.",
                      rows=[routes._correction_row(c)], conflict=True)
    if _now() >= _parse_time(c["created_at"]) + UNDO_WINDOW:
        raise Refusal(422, "Undo not available", "The undo window has passed.", "undo_token", problem=True)
    args = {"correction_id": cid, **({"reason": intent["reason"]} if intent.get("reason") else {})}
    done = _call(corrections.retire_trip_correction, args, caller, door="human", idempotency_key=key, via="undo")
    return _receipt(routes, [routes._correction_row(done)])


_KINDS = {"create": _create, "retire": _retire, "undo": _undo}


def _rejected(routes, exc):
    problem = {"type": f"about:blank#{exc.title.lower().replace(' ', '-')}", "title": exc.title, "status": exc.status,
               "detail": exc.detail}
    if exc.field is not None:
        problem["field"] = exc.field
    try:
        _, _, vintage = routes._correction_rows({})
    except Exception:  # noqa: BLE001 - the refusal stands without a vintage read
        vintage = ""
    return {"status": "conflict" if exc.conflict else "rejected", "rows": exc.rows or [], "problems": [problem],
            "undo": None, "vintage": vintage, "refreshed": [], "stale": None}


def _body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode()
    try:
        body = json.loads(raw) if raw else None
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise Refusal(400, "Invalid request", "The intent must be a JSON object.")
    return body


def intent(routes, set_id, event, claims):
    """The response for POST /recordsets/{set_id}/intents."""
    caller = str(claims.get("sub") or "")
    try:
        if set_id != SET:
            raise Refusal(405, "Method not allowed", "This set takes no writes.")
        body = _body(event)
        headers = {str(k).lower(): str(v) for k, v in (event.get("headers") or {}).items() if v is not None}
        key = str(headers.get("idempotency-key") or body.get("idempotency_key") or "").strip()
        if not key:
            raise Refusal(400, "Invalid request", "An intent carries an Idempotency-Key.", "idempotency_key")
        kind = body.get("kind")
        if kind not in _KINDS:
            raise Refusal(422, "Write refused", "This set takes create, retire and undo.", "kind", problem=True)
        if kind != "undo" and not (isinstance(body.get("reason"), str) and body["reason"].strip()):
            raise Refusal(422, "Write refused", "A reason is required.", "reason")
        return routes._resp(200, _KINDS[kind](routes, body, caller, key))
    except Refusal as exc:
        if (exc.problem or exc.status in (400, 405)) and not exc.conflict:
            extra = {"allow": "GET"} if exc.status == 405 else None
            return routes.problem(exc.status, exc.title, exc.detail, exc.field, extra=extra)
        return routes._resp(exc.status, _rejected(routes, exc))


# --------------------------------------------------------------------------- writes log
def writes(routes, set_id, claims):
    """The body of GET /recordsets/{set_id}/writes: oldest first, as the contract says (a view shows them newest first).
    The trips set is computed and has no writes: its log is empty, never a 404."""
    if set_id != SET:
        return []
    caller = str(claims.get("sub") or "")
    audit = corrections.load_correction_audit()
    out = []
    for c in corrections.load_corrections(None, include_retired=True):
        cid, a = c["correction_id"], audit.get(c["correction_id"], {})
        undo, closed = _undo_for(c, a, caller)
        entry = {"idempotency_key": a.get("idempotency_key") or f"create:{cid}", "kind": "create", "key": cid,
                 "at": c["created_at"], "vintage": routes._vintage("write", cid, c["created_at"]),
                 "principal": principal_word(a.get("door") or "agent"), "door": a.get("door") or "agent", "undo": undo}
        if c.get("reason"):
            entry["reason"] = c["reason"]
        if closed:
            entry["undo_closed"] = closed
        out.append(entry)
        if c["status"] == "retired":
            via = a.get("retired_via") or "retire"
            entry = {"idempotency_key": a.get("retire_idempotency_key") or f"retire:{cid}",
                     "kind": "undo" if via == "undo" else "retire", "key": cid, "at": c["retired_at"],
                     "vintage": routes._vintage("write", cid, c["retired_at"]),
                     "principal": principal_word(a.get("retired_door") or "agent"), "door": a.get("retired_door") or "agent", "undo": None,
                     "undo_closed": "is_undo" if via == "undo" else "no_inverse"}
            if a.get("retire_reason"):
                entry["reason"] = a["retire_reason"]
            out.append(entry)
    return sorted(out, key=lambda e: (e["at"] or "", e["kind"] != "create", e["idempotency_key"]))
