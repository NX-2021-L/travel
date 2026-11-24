"""Lane 2E (FLY-TSK-029-0E): trip corrections. BRD DOC-14994CDDA217 section 6.6, acceptance A-11.

Tools: record_trip_correction, retire_trip_correction.

A correction is a STORED FACT, never a patch on a computed trip. Trips are computed on read; the trip
engine (lane 2D) takes records, residence periods, home places and corrections and returns trips. This
module owns the corrections' storage and the one small surface the engine and the read path use:

    load_corrections(table=None, include_retired=False) -> list[dict]   # storage read, paginated Scan
    engine_corrections(table=None)                      -> list[dict]   # ACTIVE corrections, deterministic order

Each returned dict: correction_id, kind (returned|left|together|leave_out), date, record_ids (sorted),
reason, author, session, question_id, status, created_at, retired_at, retired_by.
Retiring flips status to "retired"; the row stays. Dropping retired rows from the engine's input is
what reopens the trip: the rebuild without the correction is the pre-correction shape.

Storage note: every records-table item carries the record kind in attribute `kind` (the listing is a
Scan filtered on it), so a correction row has kind="correction" and keeps the correction kind in
`correction_kind`. The dicts above present it back as `kind`, the contract's field name.

Nothing in this module deletes an item (tests/test_lane_2e_corrections.py proves it).
"""
import json
import os
import sys
from datetime import date as _date, datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError

import contract
import settings as _settings

RECORD_KIND = "correction"
_HELD_KINDS = ("lodging", "car_rental", "drive")  # the records-table kinds the trip engine reads
TABLE_ENV = "RECORDS_TABLE_NAME"


def _lf():
    """The dispatcher module (error/success/dry-run helpers, principal). Looked up lazily: it imports us."""
    mod = sys.modules.get("lambda_function")
    if mod is None:
        import lambda_function as mod  # noqa: F811
    return mod


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class _Fail(Exception):
    def __init__(self, code, message, field=None):
        super().__init__(message)
        self.code, self.message, self.field = code, message, field


def _failed(lf, f):
    """The tool error for a refusal; `field` names the argument at fault, so the records door reports a field problem."""
    out = lf._error(f.code, f.message, status=contract.ERROR_CODES[f.code])
    if f.field:
        body = json.loads(out[0].text)
        body["error_envelope"]["field"] = f.field
        out[0].text = json.dumps(body)
    return out


# --------------------------------------------------------------------------- the records door's write context
# FLY-TSK-087 (BRD DOC-D14184D324F0 6.7): think's records door writes through the two handlers below and nothing else.
# It sets this context for one call: the door that carried the write, the intent's idempotency key, and for a retire
# whether it undoes a create. A create with a key gets the id derived from the key, so a replay finds the same row.
# The MCP tools never set it; their writes carry no door attribute and are read back as the agent door.
_WRITE_CONTEXT = {}


class door_write:
    """with door_write(door="human", idempotency_key=k[, via="undo"]): one write through the records door."""

    def __init__(self, **ctx):
        self.ctx = ctx

    def __enter__(self):
        _WRITE_CONTEXT.clear()
        _WRITE_CONTEXT.update(self.ctx)
        return self

    def __exit__(self, *exc):
        _WRITE_CONTEXT.clear()
        return False


def _table():
    name = os.environ.get(TABLE_ENV, "").strip()
    if not name:
        raise _Fail("missing", f"{TABLE_ENV} is not set on this deployment, so corrections cannot be stored or read; "
                               "the records table is supplied by the deployment (FLY-TSK-041).")
    try:
        region = _settings.region()
    except _settings.SettingsError as exc:  # no default region exists (A-14): fail closed
        raise _Fail("missing", f"{exc}. Nothing was read or written.")
    return boto3.resource("dynamodb", region_name=region).Table(name)


def _unreachable(exc):
    return _Fail("missing", f"The records table named by {TABLE_ENV} ({os.environ.get(TABLE_ENV)}) is not reachable: {exc}")


# --------------------------------------------------------------------------- the engine-facing surface
_CONFIRMED_ONLY = ("first_date", "last_date", "method", "supersedes")


def _present(item):
    out = {
        "correction_id": item.get("record_id"), "kind": item.get("correction_kind"), "date": item.get("date"),
        "date_iso": item.get("date_iso"),
        "record_ids": list(item.get("record_ids") or []), "reason": item.get("reason"),
        "author": item.get("author"), "session": item.get("session"), "question_id": item.get("question_id"),
        "status": item.get("status"), "created_at": item.get("created_at"),
        "retired_at": item.get("retired_at"), "retired_by": item.get("retired_by"),
    }
    if item.get("correction_kind") == "confirmed":  # the four fields of a confirmation; other kinds keep their shape
        out.update(first_date=item.get("first_date"), last_date=item.get("last_date"), method=item.get("method"),
                   supersedes=sorted(item.get("supersedes") or []))
    return out


_AUDIT = ("idempotency_key", "door", "retire_reason", "retired_door", "retired_via", "retire_idempotency_key")


def load_correction_audit(table=None):
    """{correction_id: {audit attribute: value}} for the records door's writes log (FLY-TSK-087). Retired included."""
    table = table if table is not None else _table()
    kwargs = {"FilterExpression": Attr("kind").eq(RECORD_KIND)}
    out = {}
    try:
        while True:
            page = table.scan(**kwargs)
            for i in page.get("Items", []):
                out[i.get("record_id")] = {k: i[k] for k in _AUDIT if i.get(k) is not None}
            if not page.get("LastEvaluatedKey"):
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    except (ClientError, BotoCoreError) as exc:
        raise _unreachable(exc)
    return out


def load_corrections(table=None, include_retired=False):
    """Every correction (active only unless include_retired), paginated, in a stable order."""
    table = table if table is not None else _table()
    kwargs = {"FilterExpression": Attr("kind").eq(RECORD_KIND)}
    items = []
    try:
        while True:
            page = table.scan(**kwargs)
            items.extend(page.get("Items", []))
            if not page.get("LastEvaluatedKey"):
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    except (ClientError, BotoCoreError) as exc:
        raise _unreachable(exc)
    out = [_present(i) for i in items]
    if not include_retired:
        out = [c for c in out if c["status"] == "active"]
    return sorted(out, key=lambda c: (c["created_at"] or "", c["correction_id"] or ""))


def engine_corrections(table=None):
    """The corrections the trip engine consumes: active only, deterministic order."""
    return load_corrections(table, include_retired=False)


# --------------------------------------------------------------------------- validation
def _clean_ids(raw):
    if not isinstance(raw, list) or not raw or not all(isinstance(r, str) and r.strip() for r in raw):
        raise _Fail("invalid", "record_ids must be a non-empty list of record id strings", "record_ids")
    return sorted({r.strip() for r in raw})


def _validate(args):
    kind = args.get("kind")
    if kind not in contract.CORRECTION_KINDS:
        raise _Fail("invalid", f"kind must be one of {', '.join(contract.CORRECTION_KINDS)}", "kind")
    ids = _clean_ids(args.get("record_ids"))
    when = args.get("date")
    if kind in ("returned", "left") and not when:
        raise _Fail("invalid", f"date is required for a '{kind}' correction", "date")
    if when is not None:
        try:
            _date.fromisoformat(str(when))
        except ValueError:
            raise _Fail("invalid", "date must be an ISO date (YYYY-MM-DD)", "date")
    if kind == "together" and len(ids) < 2:
        raise _Fail("invalid", "a 'together' correction needs at least two record_ids", "record_ids")
    _validate_confirmed(args, kind)
    for opt in ("reason", "question_id", "session"):
        if args.get(opt) is not None and not isinstance(args[opt], str):
            raise _Fail("invalid", f"{opt} must be a string", opt)
    return kind, str(when) if when else "", ids


def _validate_confirmed(args, kind):
    """kind confirmed: first_date, last_date, method, optional supersedes. No other kind takes them."""
    if kind != "confirmed":
        given = [k for k in _CONFIRMED_ONLY if args.get(k) is not None]
        if given:
            raise _Fail("invalid", f"{', '.join(given)} belong to a 'confirmed' correction only, not '{kind}'", given[0])
        return
    days = {}
    for f in ("first_date", "last_date"):
        try:
            days[f] = _date.fromisoformat(str(args.get(f)))
        except ValueError:
            raise _Fail("invalid", f"{f} is required for a 'confirmed' correction and must be an ISO date (YYYY-MM-DD)", f)
    if days["first_date"] > days["last_date"]:
        raise _Fail("invalid", "first_date must not be after last_date", "last_date")
    if args.get("method") not in contract.CONFIRMATION_METHODS:
        raise _Fail("invalid", f"method must be one of {', '.join(contract.CONFIRMATION_METHODS)}", "method")
    sup = args.get("supersedes")
    if sup is not None and (not isinstance(sup, list) or not all(isinstance(x, str) and x.startswith(RECORD_KIND + "-")
                                                                 and len(x) > len(RECORD_KIND) + 1 for x in sup)):
        raise _Fail("invalid", "supersedes must be a list of confirmation ids (correction-<hash16>)", "supersedes")


def check_record_ids_exist(ids, table):
    """A correction may name only records the engine can hold: a flight row or a lodging, car rental or drive
    record, cancelled ones included (the engine holds those with a disposition). Raises invalid, naming the unknown ids."""
    lf, unknown = _lf(), []
    try:
        for rid in ids:
            kind, _, rest = rid.partition("-")
            if kind in _HELD_KINDS and rest:
                item = table.get_item(Key={"pk": f"{kind}#{rest}", "sk": contract.SK_CURRENT}, ConsistentRead=True).get("Item")
            else:
                item = lf._get_table().get_item(Key={"flight_id": rid}, ConsistentRead=True).get("Item")
            if not item:
                unknown.append(rid)
    except (ClientError, BotoCoreError) as exc:
        raise _unreachable(exc)
    if unknown:
        raise _Fail("invalid", f"No such record: {', '.join(unknown)}. A correction names only records that exist. Nothing was written.",
                    "record_ids")


def check_supersedes_exist(args, table):
    """supersedes names only existing corrections of kind confirmed (retired ones are fine). Raises invalid."""
    unknown = []
    try:
        for cid in sorted(set(args.get("supersedes") or [])):
            item = table.get_item(Key={"pk": f"{RECORD_KIND}#{cid[len(RECORD_KIND) + 1:]}", "sk": contract.SK_CURRENT},
                                  ConsistentRead=True).get("Item")
            if not item or item.get("correction_kind") != "confirmed":
                unknown.append(cid)
    except (ClientError, BotoCoreError) as exc:
        raise _unreachable(exc)
    if unknown:
        raise _Fail("invalid", f"supersedes names no confirmation: {', '.join(unknown)}. Nothing was written.", "supersedes")


def _author():
    principal = _lf()._PRINCIPAL.get() or {}
    who = principal.get("id")
    if not who:
        raise _Fail("not_allowed", "no authenticated principal: a correction is stamped with its author")
    return str(who)


def _build_item(args, kind, when, ids):
    stamp = {"author": _author()}
    if _lf().stamp_session(args):  # the session is optional; absent means the attribute is absent
        stamp["session"] = _lf().stamp_session(args)
    created = _now()
    idem = _WRITE_CONTEXT.get("idempotency_key")
    keys = contract.record_keys(RECORD_KIND, ["intent", idem] if idem else [kind, when, ",".join(ids), created])
    item = {**keys, "kind": RECORD_KIND, "correction_kind": kind, "status": "active",
            "date_iso": when or created[:10], "record_ids": ids, "created_at": created, "updated_at": created, **stamp}
    if idem:
        item.update(idempotency_key=idem, door=_WRITE_CONTEXT.get("door"))
    if when:
        item["date"] = when
    if kind == "confirmed":
        item.update(first_date=args["first_date"], last_date=args["last_date"], method=args["method"])
        if args.get("supersedes"):
            item["supersedes"] = sorted(set(args["supersedes"]))
    for opt in ("reason", "question_id"):
        if args.get(opt):
            item[opt] = args[opt]
    return item


_INTENT_FIELDS = ("correction_kind", "date", "record_ids", "reason", "question_id", "first_date", "last_date", "method",
                  "supersedes", "author")


def _same_intent(held, item):
    return all(held.get(k) == item.get(k) for k in _INTENT_FIELDS)


# --------------------------------------------------------------------------- handlers
async def record_trip_correction(arguments):
    lf = _lf()
    args, dry_run, idem_key = lf._split_control(arguments or {})
    try:
        kind, when, ids = _validate(args)
        table = _table()  # fail early and by name if the setting is missing, for the dry run too
        check_record_ids_exist(ids, table)  # unknown ids are refused, dry run included
        check_supersedes_exist(args, table)
        item = _build_item(args, kind, when, ids)
        if dry_run:
            return lf._resolved_call(
                "record_trip_correction", args, idem_key,
                [{"handler": "dynamodb", "method": "PutItem", "path": f"{table.name}/{item['pk']}/{item['sk']}", "body": item}],
                ["dynamodb:PutItem"],
                placeholders=[{"pointer": "/result/correction_id", "label": "server-minted correction id"},
                              {"pointer": "/result/created_at", "label": "server-minted timestamp"}],
                sentence=f"Record a '{kind}' correction over {len(ids)} record(s), authored by {item['author']}",
            )
        try:
            table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                held = item.get("idempotency_key") and table.get_item(Key={"pk": item["pk"], "sk": item["sk"]},
                                                                       ConsistentRead=True).get("Item")
                if held and _same_intent(held, item):  # a replay of one records-door intent: the row it made
                    return lf._success(_present(held))
                return lf._error("duplicate", f"Correction {item['record_id']} already exists.", status=409)
            raise _unreachable(exc)
        except BotoCoreError as exc:
            raise _unreachable(exc)
        return lf._success(_present(item))
    except _Fail as f:
        return _failed(lf, f)


async def retire_trip_correction(arguments):
    lf = _lf()
    args, dry_run, idem_key = lf._split_control(arguments or {})
    try:
        cid = args.get("correction_id")
        prefix = RECORD_KIND + "-"
        if not isinstance(cid, str) or not cid.startswith(prefix) or len(cid) == len(prefix):
            raise _Fail("invalid", "correction_id must be a correction-<hash16> record id", "correction_id")
        for opt in ("reason", "session"):
            if args.get(opt) is not None and not isinstance(args[opt], str):
                raise _Fail("invalid", f"{opt} must be a string", opt)
        who = _author()
        table = _table()
        key = {"pk": f"{RECORD_KIND}#{cid[len(prefix):]}", "sk": contract.SK_CURRENT}
        try:
            item = table.get_item(Key=key, ConsistentRead=True).get("Item")
        except (ClientError, BotoCoreError) as exc:
            raise _unreachable(exc)
        if not item:
            raise _Fail("missing", f"No correction {cid}.")
        if item.get("status") == "retired":  # idempotent: nothing to write
            if dry_run:
                return lf._resolved_call("retire_trip_correction", args, idem_key, [], [],
                                         sentence=f"{cid} is already retired; nothing would change")
            return lf._success(_present(item))
        now = _now()
        sets = {"status": "retired", "retired_at": now, "retired_by": who, "updated_at": now}
        if lf.stamp_session(args):
            sets["retired_session"] = lf.stamp_session(args)
        if args.get("reason"):
            sets["retire_reason"] = args["reason"]
        if _WRITE_CONTEXT.get("idempotency_key"):
            sets.update(retired_door=_WRITE_CONTEXT.get("door"), retired_via=_WRITE_CONTEXT.get("via") or "retire",
                        retire_idempotency_key=_WRITE_CONTEXT["idempotency_key"])
        if dry_run:
            return lf._resolved_call(
                "retire_trip_correction", args, idem_key,
                [{"handler": "dynamodb", "method": "UpdateItem", "path": f"{table.name}/{key['pk']}/{key['sk']}", "body": {"set": sets}}],
                ["dynamodb:UpdateItem"],
                diff=[{"pointer": "/status", "before": item.get("status"), "after": "retired"}],
                sentence=f"Retire correction {cid}, reopening what it closed; the correction is kept",
            )
        names = {f"#{k}": k for k in sets}
        try:
            resp = table.update_item(
                Key=key, UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in sets),
                ConditionExpression="#status = :active", ReturnValues="ALL_NEW",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues={**{f":{k}": v for k, v in sets.items()}, ":active": "active"})
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                item = table.get_item(Key=key, ConsistentRead=True).get("Item") or item
                return lf._success(_present(item))  # retired concurrently: same end state
            raise _unreachable(exc)
        return lf._success(_present(resp["Attributes"]))
    except _Fail as f:
        return _failed(lf, f)


HANDLERS = {
    "record_trip_correction": record_trip_correction,
    "retire_trip_correction": retire_trip_correction,
}
