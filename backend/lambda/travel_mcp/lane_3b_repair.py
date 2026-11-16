"""Lane 3B (FLY-TSK-030-0B): prepared questions and guided repair. BRD DOC-14994CDDA217 section 6.6 (B5), acceptance A-13.

Tools: list_undecided_trips, answer_trip_question.

The agent relays a question and an answer and works nothing out: every question, its two to four answers,
and the exact correction each answer writes are computed HERE from the trips the engine returns. Choosing an
answer writes one correction, through lane 2E's record_trip_correction tool path (this module never writes a
correction row itself).

One question per flag and per signal: contract.QUESTIONS maps each name to a question id, and _BUILDERS holds
the wording and the answers for each id. tests/test_lane_3b_repair.py asserts the mapping is total.

A trip is CLOSED when it has no open question. A question is open while its flag or signal is on the trip AND no
active correction carrying that question_id touches the trip's records. Boundary flags clear by themselves once
the correction is written (the engine reads the boundary as `recorded`); signals and `is this place home` are
recomputed from the records every time, so they close by being answered. Retiring the correction reopens the
question. The one question with no travel-side answer (q_home_not_recorded) stays open until finance has a home.

A trip_key is the id of the trip's first record and is recomputed on every read, so it can go stale between the
list and the answer. answer_trip_question therefore resolves the key afresh and refuses (missing) unless it still
names a trip on which that question is open; nothing is written against a key that no longer resolves.

Two checks the correction store does not make, made here before anything is written:
  * question_id must be one of contract.QUESTIONS' ids
  * every record id the correction names must exist (flights table or records table) and not be cancelled
"""
import asyncio
import datetime as _dt
import os
import sys
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError

import contract
import home_service
import lane_2e_corrections as corrections_lane
import trip_engine as te

DEFAULT_LIMIT = 50
_RECORD_KINDS = ("lodging", "car_rental", "drive")
_HOME_KWARGS = {}  # transport / credential / endpoint for home_service.read_home; a deployment supplies none


def _lf():
    mod = sys.modules.get("lambda_function")
    if mod is None:
        import lambda_function as mod  # noqa: F811
    return mod


class _Fail(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


def _today():
    return _dt.datetime.now(_dt.timezone.utc).date().isoformat()


def _plus(iso, days):
    return (_dt.date.fromisoformat(iso) + _dt.timedelta(days=days)).isoformat()


def _native(v):
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, dict):
        return {k: _native(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_native(x) for x in v]
    return v


# --------------------------------------------------------------------------- reading the inputs
def _records_table():
    name = os.environ.get("RECORDS_TABLE_NAME", "").strip()
    if not name:
        raise _Fail("missing", "RECORDS_TABLE_NAME is not set on this deployment, so trips cannot be read.")
    return boto3.resource("dynamodb", region_name=_lf().REGION).Table(name)


def _scan(table, **kwargs):
    items = []
    while True:
        page = table.scan(**kwargs)
        items.extend(page.get("Items", []))
        if not page.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _record_dates(rec):
    for f in ("date_iso", "first_night", "last_night", "pickup_date", "dropoff_date", "date"):
        v = rec.get(f)
        if isinstance(v, str) and len(v) >= 10:
            yield v[:10]


def _home_periods(read):
    out = []
    for p in read.periods:
        if p.end_status == "ended":
            last = (p.effective_to - _dt.timedelta(days=1)).isoformat()
        elif p.end_status == "end_unknown":
            last = p.effective_from.isoformat()
        else:
            last = None
        out.append({"residence_uid": p.residence_uid, "first_night": p.effective_from.isoformat(), "last_night": last,
                    "state": p.state, "country": p.country, "tenure": p.tenure})
    return out


def _snapshot():
    """Read everything the engine needs and compute: the read path, nothing carried over between calls."""
    lf = _lf()
    try:
        rtable = _records_table()
        flights = [_native(i) for i in _scan(lf._get_table()) if i.get("flight_id")]
        rows = [_native(i) for i in _scan(rtable, FilterExpression=Attr("kind").is_in(list(_RECORD_KINDS) + ["home_places"]))]
        corrections = corrections_lane.engine_corrections(rtable)
    except (ClientError, BotoCoreError) as exc:
        raise _Fail("missing", f"The flights or records table is not reachable: {exc}")
    except corrections_lane._Fail as f:
        raise _Fail(f.code, f.message)
    records = flights + [r for r in rows if r.get("kind") in _RECORD_KINDS]
    home_places = [r for r in rows if r.get("kind") == "home_places"]
    today = _today()
    dates = sorted(d for r in records for d in _record_dates(r))
    if not dates:
        return {"trips": [], "corrections": corrections, "today": today, "excluded": [], "unapplied": []}
    try:
        read = home_service.read_home(dates[0], dates[-1], **_HOME_KWARGS)
    except home_service.HomeError as e:
        raise _Fail(e.code, e.message)
    res = te.compute_trips(records, _home_periods(read), today=today, home_places=home_places, corrections=corrections)
    return {"trips": res["trips"], "corrections": corrections, "today": today,
            "excluded": res["excluded"], "unapplied": res["unapplied_corrections"]}


# --------------------------------------------------------------------------- the questions
def _where(t):
    seen = []
    for leg in t["legs"]:
        for p in (leg["from"], leg["to"]):
            if p and p not in seen:
                seen.append(p)
    return " to ".join(seen[:4]) if seen else "the recorded stays"


def _c(kind, ids, date=None):
    out = {"kind": kind}
    if date:
        out["date"] = date
    out["record_ids"] = list(ids)
    return out


def _ans(answer_id, label, correction):
    return {"answer_id": answer_id, "label": label, "correction": correction}


def _span(t):
    return f"{t['first_date']} to {t['last_date']}"


def _q_start(t, ctx):
    ev, first = t["first_evidence_date"], [t["record_ids"][0]]
    return (f"The trip {_where(t)} first shows up on {ev}, but nothing records when you left home. When did you leave?",
            [_ans("left_on_first_record", f"I left on {ev}, the day of the first record", _c("left", first, ev)),
             _ans("left_day_before", f"I left the day before, on {_plus(ev, -1)}", _c("left", first, _plus(ev, -1))),
             _ans("not_a_trip", "This is not a trip: leave these records out", _c("leave_out", t["record_ids"]))])


def _q_return(t, ctx):
    ev, last = t["last_evidence_date"], [t["record_ids"][-1]]
    return (f"The trip {_where(t)} last shows up on {ev}, but nothing records when you got home. When did you get back?",
            [_ans("back_on_last_record", f"I got back on {ev}, the day of the last record", _c("returned", last, ev)),
             _ans("back_day_after", f"I got back the day after, on {_plus(ev, 1)}", _c("returned", last, _plus(ev, 1))),
             _ans("not_a_trip", "This is not a trip: leave these records out", _c("leave_out", t["record_ids"]))])


def _q_return_planned(t, ctx):
    ev, last = t["last_evidence_date"], [t["record_ids"][-1]]
    return (f"The trip {_where(t)} has bookings until {ev} and no way home booked yet. When do you expect to be back?",
            [_ans("back_after_last_booking", f"On {ev}, when the last booking ends", _c("returned", last, ev)),
             _ans("back_day_after", f"The day after, on {_plus(ev, 1)}", _c("returned", last, _plus(ev, 1))),
             _ans("not_going", "It is not happening: leave these records out", _c("leave_out", t["record_ids"]))])


def _q_home_not_recorded(t, ctx):
    return ("Finance has no home recorded for some of these dates, so travel cannot tell where home was. "
            "Record where you lived in finance; there is nothing to answer here.", [])


def _confirm_date(t):
    return t["last_date"] if t["last_basis"] != "undecided" else t["last_evidence_date"]


def _q_place_home(t, ctx):
    return (f"Part of the trip {_where(t)} ({_span(t)}) is close to home but is not on your home list. Is it really a trip away?",
            [_ans("away_trip_stands", "Yes, it is a real trip away: keep it as shown",
                  _c("returned", [t["record_ids"][-1]], _confirm_date(t))),
             _ans("is_home", "No, that place is home: this is not a trip, leave these records out",
                  _c("leave_out", t["record_ids"]))])


def _q_duplicate(t, ctx):
    a, b = ctx["twins"][t["key"]]
    (ad, aid), (bd, bid) = a, b
    where = f"{aid} ({ad}) and {bid} ({bd})"
    answers = [_ans("leave_out_later", f"They are one journey: leave out {bid}, the later record", _c("leave_out", [bid])),
               _ans("leave_out_earlier", f"They are one journey: leave out {aid}, the earlier record", _c("leave_out", [aid]))]
    if ctx["trip_of"][aid] != ctx["trip_of"][bid]:
        answers.append(_ans("same_trip", "They belong to one trip: join the two trips", _c("together", [aid, bid])))
    return f"{where} run the same route within a day of each other. Are they the same journey recorded twice?", answers


def _q_long(t, ctx):
    return (f"The trip {_where(t)} runs {t['nights']} nights ({_span(t)}), much longer than your usual trips. Is that right?",
            [_ans("long_is_right", "Yes, it was that long: keep it as shown",
                  _c("returned", [t["record_ids"][-1]], _confirm_date(t))),
             _ans("not_a_trip", "No, this is not a trip: leave these records out", _c("leave_out", t["record_ids"]))])


_BUILDERS = {
    "q_start_not_recorded": _q_start,
    "q_return_not_recorded": _q_return,
    "q_return_not_planned_yet": _q_return_planned,
    "q_home_not_recorded": _q_home_not_recorded,
    "q_is_this_place_home": _q_place_home,
    "q_likely_duplicate": _q_duplicate,
    "q_unusually_long": _q_long,
}


def _twins(trips):
    """For each trip with 'likely duplicate': its first pair of same-route legs within a day, as ((date, id), (date, id))."""
    legs = sorted(((l["from"] or "", l["to"] or "", l["date"], t["key"], l["record_id"]) for t in trips for l in t["legs"]))
    out = {}
    for x, y in zip(legs, legs[1:]):
        if x[:2] == y[:2] and x[4] != y[4] and abs((_dt.date.fromisoformat(x[2]) - _dt.date.fromisoformat(y[2])).days) <= 1:
            pair = tuple(sorted(((x[2], x[4]), (y[2], y[4]))))
            for key in (x[3], y[3]):
                out.setdefault(key, pair)
    return out


def _context(snap):
    trips = snap["trips"]
    return {"twins": _twins(trips), "trip_of": {r: t["key"] for t in trips for r in t["record_ids"]}}


def _answered(snap, trip, qid):
    ids = set(trip["record_ids"])
    return any(c.get("question_id") == qid and ids & set(c.get("record_ids") or []) for c in snap["corrections"])


def _open_questions(snap, trip, ctx):
    """The trip's open questions: one per flag and signal still on it, unless already answered."""
    out = []
    for name in list(trip["flags"]) + list(trip["signals"]):
        qid = contract.QUESTIONS[name]
        if _answered(snap, trip, qid):
            continue
        text, answers = _BUILDERS[qid](trip, ctx)
        assert not answers or contract.QUESTION_ANSWERS_MIN <= len(answers) <= contract.QUESTION_ANSWERS_MAX
        out.append({"question_id": qid, "trip_key": trip["key"], "text": text, "answers": answers})
    return out


# --------------------------------------------------------------------------- validation
def _validate_record_ids(ids):
    """Every record id a correction names must exist and not be cancelled. Raises _Fail('missing')."""
    lf, rtable = _lf(), _records_table()
    absent = []
    try:
        for rid in ids:
            kind, _, rest = rid.partition("-")
            if kind in _RECORD_KINDS and rest:
                item = rtable.get_item(Key={"pk": f"{kind}#{rest}", "sk": contract.SK_CURRENT}, ConsistentRead=True).get("Item")
            else:
                item = lf._get_table().get_item(Key={"flight_id": rid}, ConsistentRead=True).get("Item")
            if not item or item.get("status") == "cancelled":
                absent.append(rid)
    except (ClientError, BotoCoreError) as exc:
        raise _Fail("missing", f"The flights or records table is not reachable: {exc}")
    if absent:
        raise _Fail("missing", f"No such record, or it is cancelled: {', '.join(absent)}. Nothing was written.")


def _text(args, name, required=True):
    v = args.get(name)
    if v is None and not required:
        return None
    if not isinstance(v, str) or not v.strip():
        raise _Fail("invalid", f"{name} is required and must be a non-empty string")
    return v.strip()


def _summary(t):
    return {k: t[k] for k in ("key", "first_date", "first_basis", "last_date", "last_basis", "nights", "state", "flags",
                              "signals", "corrections")}


# --------------------------------------------------------------------------- handlers
async def list_undecided_trips(arguments):
    lf = _lf()
    args = arguments or {}
    try:
        limit = args.get("limit", DEFAULT_LIMIT)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise _Fail("invalid", "limit must be a positive integer")
        for f in ("date_from", "date_to"):
            if args.get(f) is not None:
                try:
                    _dt.date.fromisoformat(str(args[f]))
                except ValueError:
                    raise _Fail("invalid", f"{f} must be an ISO date (YYYY-MM-DD)")
        snap = await asyncio.to_thread(_snapshot)
        ctx = _context(snap)
        trips = te.order_trips_for_listing(te.trips_in_range(snap["trips"], args.get("date_from"), args.get("date_to")))
        out = []
        for t in trips:
            qs = _open_questions(snap, t, ctx)
            if qs:
                out.append({**_summary(t), "questions": qs})
        return lf._success({"trips": out[:limit], "count": len(out)})
    except _Fail as f:
        return lf._error(f.code, f.message, status=contract.ERROR_CODES[f.code])


async def answer_trip_question(arguments):
    lf = _lf()
    args, dry_run, idem_key = lf._split_control(arguments or {})
    try:
        trip_key, qid, aid = _text(args, "trip_key"), _text(args, "question_id"), _text(args, "answer_id")
        session = _text(args, "session", required=False)
        if qid not in contract.QUESTIONS.values():
            raise _Fail("invalid", f"question_id {qid} is not a prepared question; one of: {', '.join(sorted(contract.QUESTIONS.values()))}")
        if qid in contract.QUESTIONS_WITHOUT_TRAVEL_ANSWER:
            raise _Fail("invalid", f"{qid} has no travel-side answer: it is settled in finance, not here.")
        snap = await asyncio.to_thread(_snapshot)
        trip = next((t for t in snap["trips"] if t["key"] == trip_key), None)
        if trip is None:
            raise _Fail("missing", f"No trip {trip_key}. A trip's key is its first record's id and can change after a correction; list again.")
        if _answered(snap, trip, qid):
            raise _Fail("duplicate", f"{qid} on trip {trip_key} already has an active correction; retire it to ask again.")
        q = next((x for x in _open_questions(snap, trip, _context(snap)) if x["question_id"] == qid), None)
        if q is None:
            raise _Fail("missing", f"{qid} is not open on trip {trip_key}.")
        ans = next((a for a in q["answers"] if a["answer_id"] == aid), None)
        if ans is None:
            raise _Fail("invalid", f"answer_id {aid} is not offered; one of: {', '.join(a['answer_id'] for a in q['answers'])}")
        corr = ans["correction"]
        try:
            await asyncio.to_thread(corrections_lane.check_record_ids_exist, corr["record_ids"], _records_table())
        except corrections_lane._Fail as f:
            raise _Fail(f.code, f.message)
        await asyncio.to_thread(_validate_record_ids, corr["record_ids"])
        call = {**corr, "reason": f"{qid}: {ans['label']}", "question_id": qid}
        if session:
            call["session"] = session
        record = corrections_lane.HANDLERS["record_trip_correction"]
        if dry_run:
            raw = await record({**call, "dry_run": True, "idempotency_key": idem_key})
            inner = _loads(raw)
            if not inner.get("success"):
                return raw
            underlying = inner["result"]["resolved_call"]["underlying"]
            diff = None
            if corr["kind"] in ("returned", "left"):
                f = "last" if corr["kind"] == "returned" else "first"
                diff = [{"pointer": f"/{f}_date", "before": trip[f"{f}_date"], "after": corr["date"]},
                        {"pointer": f"/{f}_basis", "before": trip[f"{f}_basis"], "after": "recorded"}]
            return lf._resolved_call(
                "answer_trip_question", args, idem_key, underlying, ["dynamodb:PutItem"], diff=diff,
                placeholders=[{"pointer": "/result/correction/correction_id", "label": "server-minted correction id"}],
                sentence=f"Answer '{ans['label']}' to {qid} on trip {trip_key}: record a '{corr['kind']}' correction over "
                         f"{len(corr['record_ids'])} record(s)")
        raw = await record({**call, "idempotency_key": idem_key} if idem_key else call)
        inner = _loads(raw)
        if not inner.get("success"):
            return raw
        after = await asyncio.to_thread(_snapshot)
        ids = set(trip["record_ids"])
        now = next((t for t in after["trips"] if ids & set(t["record_ids"])), None)
        closed = now is None or not any(x["question_id"] == qid for x in _open_questions(after, now, _context(after)))
        return lf._success({"question_id": qid, "answer_id": aid, "correction": inner["result"],
                            "closed": closed, "trip": _summary(now) if now else None})
    except _Fail as f:
        return lf._error(f.code, f.message, status=contract.ERROR_CODES[f.code])
    except home_service.HomeError as e:
        return lf._error(e.code, e.message, status=e.status)


def _loads(raw):
    import json
    return json.loads(raw[0].text)


HANDLERS = {"list_undecided_trips": list_undecided_trips, "answer_trip_question": answer_trip_question}
