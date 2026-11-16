"""Lane 2A (FLY-TSK-029-0A): lodging, car rental and drive records, nine write tools and two reads.

Authority: contract.py (kinds, natural keys, tool schemas, error codes) and DOC-76D85C855CBE s5 (place identity).
Items live in the records table named by RECORDS_TABLE_NAME: pk = <kind>#<hash16>, sk = "rec".

* dry_run follows the flight tools' convention: lambda_function._split_control / _resolved_call, zero writes.
* duplicates: put_item with ConditionExpression attribute_not_exists(pk) (race-safe, no read-then-write).
* cancel is soft (status = "cancelled"). Nothing in this module deletes an item.
* a lodging stay needs city, state and country; drives and rentals have their own required fields.
* listing is a Scan filtered on `kind`; the table has no kind index by design.
"""
import base64
import json
import os
import re
import sys
from datetime import date
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

import contract

SETTING = "RECORDS_TABLE_NAME"
KINDS = ("lodging", "car_rental", "drive")
_IDENTITY_EXTRA = ("session", "place_resolution")  # accepted on create besides the contract fields
_PLACE_EXTRA = {
    "lodging": ("place_id", "area_id"),
    "car_rental": ("pickup_place_id", "pickup_area_id", "return_place_id", "return_area_id"),
    "drive": ("from_place_id", "from_area_id", "to_place_id", "to_area_id", "from_lat", "from_lon", "to_lat", "to_lon"),
}
_DATE_FIELDS = {
    "lodging": ("first_night", "last_night"),
    "car_rental": ("pickup_date", "dropoff_date"),
    "drive": ("date",),
}
_resource = None


class _Refuse(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable


def _lf():
    """The dispatcher module, resolved lazily: it imports this module while it is still loading."""
    mod = sys.modules.get("lambda_function")
    if mod is None:
        import lambda_function as mod  # noqa: F401
    return mod


def _fail(exc):
    lf = _lf()
    if exc.code == "internal_error":  # a legacy code (contract.LEGACY_ERROR_CODES): setting or table problem
        return lf._error(exc.code, exc.message, status=500, retryable=exc.retryable)
    return lf._error(exc.code, exc.message, status=contract.ERROR_CODES[exc.code])


# --------------------------------------------------------------------------- table
def _table():
    global _resource
    name = os.environ.get(SETTING, "").strip()
    if not name:
        raise _Refuse("internal_error", f"{SETTING} is not set on this deployment: the records table is not configured "
                      f"(FLY-TSK-041 provides it). Nothing was read or written.")
    if _resource is None:
        _resource = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2",
                                   config=BotoConfig(retries={"max_attempts": 3, "mode": "adaptive"}))
    return _resource.Table(name), name


def _guard(fn):
    """Run a DynamoDB call, mapping a missing or unreachable table to a structured error naming the setting."""
    try:
        return fn()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in ("ResourceNotFoundException", "AccessDeniedException", "UnrecognizedClientException"):
            raise _Refuse("internal_error", f"The records table named by {SETTING} ({os.environ.get(SETTING)}) is unreachable: {exc}",
                          retryable=True)
        raise


def _cond_failed(exc):
    return exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


# --------------------------------------------------------------------------- validation
def _blank(v):
    return v is None or (isinstance(v, str) and not v.strip())


def _slug(s):
    return re.sub(r"[^a-z0-9]+", "-", str(s).strip().casefold()).strip("-")


def _iso_date(field, v):
    try:
        if not isinstance(v, str):
            raise ValueError
        return date.fromisoformat(v.strip()[:10]).isoformat()
    except ValueError:
        raise _Refuse("invalid", f"{field} must be an ISO date (YYYY-MM-DD), got {v!r}")


def _num(field, v):
    if isinstance(v, bool) or not isinstance(v, (int, float, Decimal)):
        raise _Refuse("invalid", f"{field} must be a number")
    return Decimal(str(v))


def _text(field, v):
    if not isinstance(v, str):
        raise _Refuse("invalid", f"{field} must be a string")
    return v.strip()


def _via(v):
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise _Refuse("invalid", "via must be a list of place names")
    return [x.strip() for x in v]


def _means(v):
    if v not in contract.DRIVE_MEANS:
        raise _Refuse("invalid", f"means must be one of {', '.join(contract.DRIVE_MEANS)}")
    return v


def _end(item, args, pid_f, aid_f, derived_slug, flags):
    """Store one place-bearing end: a supplied valid pair, else a pair derived from the text and flagged unresolved."""
    pid, aid = args.get(pid_f), args.get(aid_f)
    if pid is not None and not re.match(contract.PLACE_ID_PATTERN, str(pid)):
        raise _Refuse("invalid", f"{pid_f} must look like type:code, type in {'|'.join(contract.PLACE_TYPES)}")
    if aid is not None and not re.match(contract.AREA_ID_PATTERN, str(aid)):
        raise _Refuse("invalid", f"{aid_f} must look like city:<slug>")
    if pid and aid:
        item[pid_f], item[aid_f] = pid, aid
        return
    flags.append("unresolved")
    if derived_slug:
        item[aid_f] = aid or f"city:{derived_slug}"
        item[pid_f] = pid or f"addr:{derived_slug}"


def _check_order(kind, item):
    pairs = {"lodging": ("first_night", "last_night"), "car_rental": ("pickup_date", "dropoff_date")}
    if kind in pairs:
        a, b = pairs[kind]
        if item.get(b) and item.get(a) and item[b] < item[a]:
            raise _Refuse("invalid", f"{b} must not be before {a}")


def _validate_create(kind, args):
    """Return (item_fields, natural_key_values). Raises _Refuse('invalid')."""
    spec = contract.RECORD_KINDS[kind]
    allowed = set(spec["fields"]) | set(_IDENTITY_EXTRA) | set(_PLACE_EXTRA[kind])
    unknown = sorted(set(args) - allowed)
    if unknown:
        raise _Refuse("invalid", f"unknown field(s) for {kind}: {', '.join(unknown)}")
    # A stay is refused without city, state and country (A-6); drives and rentals require only their own fields.
    missing = [f for f in spec["required"] if _blank(args.get(f))]
    if missing:
        raise _Refuse("invalid", f"{kind} requires {', '.join(spec['required'])}; missing or empty: {', '.join(missing)}")
    item = {}
    for f, v in args.items():
        if v is None or f in ("session", "status"):
            continue
        if f in _DATE_FIELDS[kind]:
            item[f] = _iso_date(f, v)
        elif f in ("lat", "lon") or f.endswith(("_lat", "_lon")):
            item[f] = _num(f, v)
        elif f == "via":
            item[f] = _via(v)
        elif f == "means":
            item[f] = _means(v)
        elif f == "sequence":
            if isinstance(v, bool) or not isinstance(v, int):
                raise _Refuse("invalid", "sequence must be an integer")
            item[f] = v
        elif f == "place_resolution":
            if v not in contract.PLACE_RESOLUTIONS:
                raise _Refuse("invalid", f"place_resolution must be one of {', '.join(contract.PLACE_RESOLUTIONS)}")
            item[f] = v
        elif f.endswith(("place_id", "area_id")):
            item[f] = v
        else:
            item[f] = _text(f, v)
    if kind == "drive":
        item.setdefault("sequence", 0)
        item.setdefault("means", "road")
    _check_order(kind, item)
    flags = []
    if kind == "lodging":
        _end(item, args, "place_id", "area_id", _slug(f"{item['city']} {item['state']} {item['country']}"), flags)
    elif kind == "car_rental":
        _end(item, args, "pickup_place_id", "pickup_area_id", _slug(item["pickup_place"]), flags)
        if item.get("dropoff_place") or args.get("return_place_id"):
            _end(item, args, "return_place_id", "return_area_id", _slug(item.get("dropoff_place", "")), flags)
    else:
        _end(item, args, "from_place_id", "from_area_id", _slug(item["origin"]), flags)
        _end(item, args, "to_place_id", "to_area_id", _slug(item["destination"]), flags)
    item.setdefault("place_resolution", "unresolved" if flags else "exact")
    return item, [item.get(f, "") for f in spec["natural_key"]]


def _stamp_new(kind, item, keyvals, args):
    spec = contract.RECORD_KINDS[kind]
    lf = _lf()
    now = lf._now_iso()
    out = dict(item)
    out.update(contract.record_keys(kind, keyvals))
    out.update({"kind": kind, "status": "active", "date_iso": item[spec["date_iso"]], "created_at": now, "updated_at": now,
                "author": str((lf._PRINCIPAL.get() or {}).get("id") or "unknown")})
    if args.get("session"):
        out["session"] = str(args["session"])
    if out.get(spec["index_area_id"]):
        out["area_id"] = out[spec["index_area_id"]]  # the area-index GSI key
    return out


def _load(kind, record_id):
    m = re.match(r"^(lodging|car_rental|drive)-([0-9a-f]{16})$", record_id.strip()) if isinstance(record_id, str) else None
    if not m or (kind and m.group(1) != kind):
        raise _Refuse("invalid", f"{record_id!r} is not a {kind or 'lodging, car_rental or drive'} record id")
    key = {"pk": f"{m.group(1)}#{m.group(2)}", "sk": contract.SK_CURRENT}
    table, _ = _table()
    item = _guard(lambda: table.get_item(Key=key, ConsistentRead=True)).get("Item")
    if not item:
        raise _Refuse("missing", f"{record_id} not found")
    return table, key, item


# --------------------------------------------------------------------------- writes
def _make_create(kind):
    tool = f"create_{kind}"

    async def create(arguments):
        lf = _lf()
        args, dry_run, idem = lf._split_control(arguments)
        try:
            item, keyvals = _validate_create(kind, args)
            full = _stamp_new(kind, item, keyvals, args)
            table, name = _table()
            if dry_run:
                dup = _guard(lambda: table.get_item(Key={"pk": full["pk"], "sk": full["sk"]}, ConsistentRead=True)).get("Item")
                return lf._resolved_call(
                    tool, args, idem,
                    [{"handler": "dynamodb", "method": "PutItem", "path": f"{name}/{full['pk']}", "body": lf._decimal_to_native(full)}],
                    ["dynamodb:PutItem"],
                    placeholders=[{"pointer": "/result/created_at", "label": "server-minted timestamp"}],
                    sentence=f"Create {kind.replace('_', ' ')} {full['record_id']}",
                    warnings=[f"{full['record_id']} already exists; execute would be rejected as duplicate"] if dup else None)
            try:
                _guard(lambda: table.put_item(Item=full, ConditionExpression="attribute_not_exists(pk)"))
            except ClientError as exc:
                if _cond_failed(exc):
                    raise _Refuse("duplicate", f"{full['record_id']} already exists (same {', '.join(contract.RECORD_KINDS[kind]['natural_key'])}). "
                                  f"Use update_{kind} to change it.")
                raise
            return lf._success(full)
        except _Refuse as exc:
            return _fail(exc)
    return create


def _make_update(kind, idarg):
    tool = f"update_{kind}"

    async def update(arguments):
        lf = _lf()
        args, dry_run, idem = lf._split_control(arguments)
        try:
            spec = contract.RECORD_KINDS[kind]
            record_id = args.pop(idarg, None)
            if _blank(record_id):
                raise _Refuse("invalid", f"{idarg} is required")
            args.pop("session", None)
            bad = sorted(set(args) - set(spec["mutable"]))
            if bad:
                raise _Refuse("invalid", f"cannot change {', '.join(bad)}; mutable fields are {', '.join(spec['mutable'])}"
                              + ("; identity fields cannot change: cancel and record again" if set(bad) & set(spec["natural_key"]) else ""))
            if not args:
                raise _Refuse("invalid", f"nothing to update; give one of {', '.join(spec['mutable'])}")
            table, key, item = _load(kind, record_id)
            if item.get("status") == "cancelled":
                raise _Refuse("not_allowed", f"{record_id} is cancelled and cannot be updated")
            new = {}
            for f, v in args.items():
                if f in _DATE_FIELDS[kind]:
                    new[f] = _iso_date(f, v)
                elif f in ("lat", "lon"):
                    new[f] = _num(f, v)
                elif f == "via":
                    new[f] = _via(v)
                elif f == "means":
                    new[f] = _means(v)
                else:
                    new[f] = _text(f, v)
            _check_order(kind, {**item, **new})
            sets = {**new, "updated_at": lf._now_iso()}
            if dry_run:
                diff = [{"pointer": f"/{f}", "before": lf._decimal_to_native(item.get(f)), "after": lf._decimal_to_native(v)}
                        for f, v in new.items()]
                return lf._resolved_call(tool, {idarg: record_id, **args}, idem,
                                         [{"handler": "dynamodb", "method": "UpdateItem", "path": f"{table.name}/{key['pk']}",
                                           "body": {"set": lf._decimal_to_native(sets)}}],
                                         ["dynamodb:UpdateItem"], diff=diff, sentence=f"Update {record_id}")
            names = {f"#f{i}": f for i, f in enumerate(sets)}
            vals = {f":v{i}": v for i, v in enumerate(sets.values())}
            expr = "SET " + ", ".join(f"#f{i} = :v{i}" for i in range(len(sets)))
            try:
                out = _guard(lambda: table.update_item(Key=key, UpdateExpression=expr, ExpressionAttributeNames=names,
                                                       ExpressionAttributeValues=vals, ConditionExpression="attribute_exists(pk)",
                                                       ReturnValues="ALL_NEW"))
            except ClientError as exc:
                if _cond_failed(exc):
                    raise _Refuse("missing", f"{record_id} not found")
                raise
            return lf._success(out["Attributes"])
        except _Refuse as exc:
            return _fail(exc)
    return update


def _make_cancel(kind, idarg):
    tool = f"cancel_{kind}"

    async def cancel(arguments):
        lf = _lf()
        args, dry_run, idem = lf._split_control(arguments)
        try:
            record_id = args.get(idarg)
            if _blank(record_id):
                raise _Refuse("invalid", f"{idarg} is required")
            table, key, item = _load(kind, record_id)
            if item.get("status") == "cancelled":  # idempotent
                if dry_run:
                    return lf._resolved_call(tool, args, idem, [], [], sentence=f"{record_id} is already cancelled; no change",
                                             warnings=["already cancelled"])
                return lf._success(item)
            if dry_run:
                return lf._resolved_call(tool, args, idem,
                                         [{"handler": "dynamodb", "method": "UpdateItem", "path": f"{table.name}/{key['pk']}",
                                           "body": {"set": {"status": "cancelled"}}}],
                                         ["dynamodb:UpdateItem"],
                                         diff=[{"pointer": "/status", "before": item.get("status"), "after": "cancelled"}],
                                         sentence=f"Cancel {record_id}")
            now = lf._now_iso()
            try:
                out = _guard(lambda: table.update_item(
                    Key=key, UpdateExpression="SET #s = :c, cancelled_at = :n, updated_at = :n",
                    ConditionExpression="attribute_exists(pk)", ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":c": "cancelled", ":n": now}, ReturnValues="ALL_NEW"))
            except ClientError as exc:
                if _cond_failed(exc):
                    raise _Refuse("missing", f"{record_id} not found")
                raise
            return lf._success(out["Attributes"])
        except _Refuse as exc:
            return _fail(exc)
    return cancel


# --------------------------------------------------------------------------- reads
async def get_travel_record(arguments):
    try:
        if _blank(arguments.get("record_id")):
            raise _Refuse("invalid", "record_id is required")
        return _lf()._success(_load(None, arguments["record_id"])[2])
    except _Refuse as exc:
        return _fail(exc)


async def search_travel_records(arguments):
    lf = _lf()
    try:
        kind = arguments.get("kind")
        if kind is not None and kind not in KINDS:
            raise _Refuse("invalid", f"kind must be one of {', '.join(KINDS)}")
        status = arguments.get("status")
        if status is not None and status not in contract.STATUSES:
            raise _Refuse("invalid", f"status must be one of {', '.join(contract.STATUSES)}")
        lo = _iso_date("date_from", arguments["date_from"]) if arguments.get("date_from") else None
        hi = _iso_date("date_to", arguments["date_to"]) if arguments.get("date_to") else None
        limit = arguments.get("limit", 50)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise _Refuse("invalid", "limit must be a positive integer")
        limit = min(limit, 200)
        start = None
        if arguments.get("next_token"):
            try:
                start = json.loads(base64.urlsafe_b64decode(str(arguments["next_token"]).encode()))
                if not isinstance(start, dict) or set(start) != {"pk", "sk"}:
                    raise ValueError
            except Exception:
                raise _Refuse("invalid", "next_token is not valid")
        # Listing a kind is a Scan filtered on `kind`: the table has no kind index, by design.
        flt = Attr("kind").eq(kind) if kind else Attr("kind").is_in(list(KINDS))
        if status:
            flt &= Attr("status").eq(status)
        if lo:
            flt &= Attr("date_iso").gte(lo)
        if hi:
            flt &= Attr("date_iso").lte(hi)
        if arguments.get("area_id"):
            flt &= Attr("area_id").eq(arguments["area_id"])
        table, _ = _table()
        items, cursor = [], None
        for _ in range(50):
            kw = {"FilterExpression": flt}
            if start:
                kw["ExclusiveStartKey"] = start
            page = _guard(lambda: table.scan(**kw))
            got = page.get("Items", [])
            room = limit - len(items)
            if len(got) > room:
                items += got[:room]
                cursor = {"pk": items[-1]["pk"], "sk": items[-1]["sk"]}
                break
            items += got
            start = page.get("LastEvaluatedKey")
            if not start:
                break
            if len(items) == limit:
                cursor = start
                break
        else:
            cursor = start
        items.sort(key=lambda i: (i.get("date_iso", ""), i["pk"]))
        out = {"count": len(items), "records": items}
        if cursor:
            out["next_token"] = base64.urlsafe_b64encode(json.dumps(cursor).encode()).decode()
        return lf._success(out)
    except _Refuse as exc:
        return _fail(exc)


HANDLERS = {"get_travel_record": get_travel_record, "search_travel_records": search_travel_records}
for _kind, _idarg in (("lodging", "lodging_id"), ("car_rental", "car_rental_id"), ("drive", "drive_id")):
    HANDLERS[f"create_{_kind}"] = _make_create(_kind)
    HANDLERS[f"update_{_kind}"] = _make_update(_kind, _idarg)
    HANDLERS[f"cancel_{_kind}"] = _make_cancel(_kind, _idarg)
