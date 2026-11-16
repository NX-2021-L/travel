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
import os
import sys
from datetime import date as _date, datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import BotoCoreError, ClientError

import contract
import settings as _settings

RECORD_KIND = "correction"
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
    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


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
def _present(item):
    return {
        "correction_id": item.get("record_id"), "kind": item.get("correction_kind"), "date": item.get("date"),
        "record_ids": list(item.get("record_ids") or []), "reason": item.get("reason"),
        "author": item.get("author"), "session": item.get("session"), "question_id": item.get("question_id"),
        "status": item.get("status"), "created_at": item.get("created_at"),
        "retired_at": item.get("retired_at"), "retired_by": item.get("retired_by"),
    }


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
        raise _Fail("invalid", "record_ids must be a non-empty list of record id strings")
    return sorted({r.strip() for r in raw})


def _validate(args):
    kind = args.get("kind")
    if kind not in contract.CORRECTION_KINDS:
        raise _Fail("invalid", f"kind must be one of {', '.join(contract.CORRECTION_KINDS)}")
    ids = _clean_ids(args.get("record_ids"))
    when = args.get("date")
    if kind in ("returned", "left") and not when:
        raise _Fail("invalid", f"date is required for a '{kind}' correction")
    if when is not None:
        try:
            _date.fromisoformat(str(when))
        except ValueError:
            raise _Fail("invalid", "date must be an ISO date (YYYY-MM-DD)")
    if kind == "together" and len(ids) < 2:
        raise _Fail("invalid", "a 'together' correction needs at least two record_ids")
    for opt in ("reason", "question_id", "session"):
        if args.get(opt) is not None and not isinstance(args[opt], str):
            raise _Fail("invalid", f"{opt} must be a string")
    return kind, str(when) if when else "", ids


def _author():
    principal = _lf()._PRINCIPAL.get() or {}
    who = principal.get("id")
    if not who:
        raise _Fail("not_allowed", "no authenticated principal: a correction is stamped with its author")
    return str(who)


def _build_item(args, kind, when, ids):
    stamp = {"author": _author()}
    if args.get("session"):  # the session is optional; absent means the attribute is absent
        stamp["session"] = args["session"]
    created = _now()
    keys = contract.record_keys(RECORD_KIND, [kind, when, ",".join(ids), created])
    item = {**keys, "kind": RECORD_KIND, "correction_kind": kind, "status": "active",
            "date_iso": when or created[:10], "record_ids": ids, "created_at": created, "updated_at": created, **stamp}
    if when:
        item["date"] = when
    for opt in ("reason", "question_id"):
        if args.get(opt):
            item[opt] = args[opt]
    return item


# --------------------------------------------------------------------------- handlers
async def record_trip_correction(arguments):
    lf = _lf()
    args, dry_run, idem_key = lf._split_control(arguments or {})
    try:
        kind, when, ids = _validate(args)
        table = _table()  # fail early and by name if the setting is missing, for the dry run too
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
                return lf._error("duplicate", f"Correction {item['record_id']} already exists.", status=409)
            raise _unreachable(exc)
        except BotoCoreError as exc:
            raise _unreachable(exc)
        return lf._success(_present(item))
    except _Fail as f:
        return lf._error(f.code, f.message, status=contract.ERROR_CODES[f.code])


async def retire_trip_correction(arguments):
    lf = _lf()
    args, dry_run, idem_key = lf._split_control(arguments or {})
    try:
        cid = args.get("correction_id")
        prefix = RECORD_KIND + "-"
        if not isinstance(cid, str) or not cid.startswith(prefix) or len(cid) == len(prefix):
            raise _Fail("invalid", "correction_id must be a correction-<hash16> record id")
        for opt in ("reason", "session"):
            if args.get(opt) is not None and not isinstance(args[opt], str):
                raise _Fail("invalid", f"{opt} must be a string")
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
        if args.get("session"):
            sets["retired_session"] = args["session"]
        if args.get("reason"):
            sets["retire_reason"] = args["reason"]
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
        return lf._error(f.code, f.message, status=contract.ERROR_CODES[f.code])


HANDLERS = {
    "record_trip_correction": record_trip_correction,
    "retire_trip_correction": retire_trip_correction,
}
