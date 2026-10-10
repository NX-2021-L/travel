"""io-travel MCP Server — Agent-accessible flight data management.

Exposes 5 tools via MCP streamable HTTP transport:
  - search_flights: filter and paginate flight records
  - get_flight: retrieve a single flight by ID
  - update_flight: modify mutable booking fields
  - cancel_flight: soft-cancel a flight (no hard deletes)

Architecture: Lambda Function URL → ASGI adapter → MCP StreamableHTTPSessionManager
Auth: Cognito OAuth 2.1 (shared User Pool with Enceladus) or static bearer token fallback
Data: DynamoDB table io-travel-flights with 3 GSIs
"""

import asyncio
import base64 as b64
import hashlib
import hmac
import json
import logging
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import anyio
import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from mcp.server import Server
from mcp.types import TextContent, Tool, ToolAnnotations

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TABLE_NAME = os.environ.get("TABLE_NAME", "io-travel-flights")
REGION = os.environ.get("AWS_REGION", "us-west-2")
MCP_TRANSPORT = os.environ.get("MCP_TRANSPORT", "streamable_http")

# Cognito OAuth
COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "")
COGNITO_CLIENT_ID = os.environ.get("COGNITO_CLIENT_ID", "")
COGNITO_CLIENT_SECRET = os.environ.get("COGNITO_CLIENT_SECRET", "")
COGNITO_DOMAIN = os.environ.get("COGNITO_DOMAIN", "")
COGNITO_REGION = os.environ.get("COGNITO_REGION", "us-east-1")

# Static bearer token fallback
MCP_API_KEY = os.environ.get("MCP_API_KEY", "")

SERVER_BASE_URL = os.environ.get("SERVER_BASE_URL", "")

# Fail-closed human principal (orchestrator ruling E2-R10): the Cognito pool also
# holds other users, so pool membership grants nothing. Only subs in this
# allow-list may use the Cognito path (reads and writes); empty/unset = denied.
COGNITO_ALLOWED_SUBS = [
    x.strip() for x in os.environ.get("COGNITO_ALLOWED_SUBS", "").split(",") if x.strip()
]
# Optional additional write requirement: a Cognito group. Empty (default) means
# allow-listed subs may write.
TRAVEL_WRITE_GROUPS = [
    g.strip() for g in os.environ.get("TRAVEL_WRITE_GROUPS", "").split(",") if g.strip()
]

logger = logging.getLogger("travel-mcp")

# ---------------------------------------------------------------------------
# DynamoDB client
# ---------------------------------------------------------------------------

_dynamo_resource = None


def _get_table():
    global _dynamo_resource
    if _dynamo_resource is None:
        _dynamo_resource = boto3.resource(
            "dynamodb",
            region_name=REGION,
            config=BotoConfig(retries={"max_attempts": 3, "mode": "adaptive"}),
        )
    return _dynamo_resource.Table(TABLE_NAME)


# ---------------------------------------------------------------------------
# Cognito JWT validation
# ---------------------------------------------------------------------------

_cognito_jwks_cache: Dict[str, Any] = {}
_cognito_jwks_fetched_at: float = 0.0
_COGNITO_JWKS_TTL = 3600  # 1 hour


def _get_cognito_jwks() -> Dict[str, Any]:
    global _cognito_jwks_cache, _cognito_jwks_fetched_at
    now = time.time()
    if _cognito_jwks_cache and (now - _cognito_jwks_fetched_at) < _COGNITO_JWKS_TTL:
        return _cognito_jwks_cache

    if not COGNITO_USER_POOL_ID:
        raise ValueError("COGNITO_USER_POOL_ID not set")

    region = COGNITO_REGION or COGNITO_USER_POOL_ID.split("_")[0]
    url = (
        f"https://cognito-idp.{region}.amazonaws.com/"
        f"{COGNITO_USER_POOL_ID}/.well-known/jwks.json"
    )

    import jwt as _jwt_mod
    from jwt.algorithms import RSAAlgorithm as _RSA

    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    with urllib.request.urlopen(url, timeout=5, context=ctx) as resp:
        data = json.loads(resp.read())

    new_cache: Dict[str, Any] = {}
    for key_data in data.get("keys", []):
        kid = key_data.get("kid")
        if kid:
            new_cache[kid] = _RSA.from_jwk(json.dumps(key_data))

    _cognito_jwks_cache = new_cache
    _cognito_jwks_fetched_at = now
    return _cognito_jwks_cache


def _verify_cognito_jwt(token: str) -> Dict[str, Any]:
    import jwt as _jwt_mod

    header = _jwt_mod.get_unverified_header(token)
    kid = header.get("kid")
    alg = header.get("alg", "RS256")
    if alg != "RS256":
        raise ValueError(f"Unexpected token algorithm: {alg}")

    key = _get_cognito_jwks().get(kid)
    if key is None:
        raise ValueError("Token key ID not found in JWKS")

    region = COGNITO_REGION or COGNITO_USER_POOL_ID.split("_", 1)[0]
    expected_issuer = f"https://cognito-idp.{region}.amazonaws.com/{COGNITO_USER_POOL_ID}"

    claims = _jwt_mod.decode(
        token,
        key,
        algorithms=["RS256"],
        issuer=expected_issuer,
        options={"verify_exp": True, "verify_aud": False},
    )

    if COGNITO_CLIENT_ID:
        token_use = str(claims.get("token_use") or "").strip().lower()
        if token_use == "access":
            cid = str(claims.get("client_id") or "")
            if not hmac.compare_digest(cid, COGNITO_CLIENT_ID):
                raise ValueError("Token client_id mismatch")
        elif token_use == "id":
            aud = str(claims.get("aud") or "")
            if not hmac.compare_digest(aud, COGNITO_CLIENT_ID):
                raise ValueError("Token audience mismatch")

    return claims


class _RequestHolder:
    """Per-invocation request state (principal, idempotency key).

    A ContextVar is NOT safe here: the MCP session manager runs tool handlers in
    tasks whose context was copied before the request principal was set, so the
    handler saw None (travel incident 2026-10-10, E2-R12).  A Lambda container
    serves one request at a time, so a module-level holder that
    _handle_lambda_event resets in a finally block is correct.  The ContextVar
    get/set/reset API is kept.
    """

    def __init__(self, default=None):
        self._default = default
        self._value = default

    def get(self):
        return self._value

    def set(self, value):
        old, self._value = self._value, value
        return old

    def reset(self, token=None):
        self._value = self._default if token is None else token


_PRINCIPAL = _RequestHolder(None)
_IDEMPOTENCY_KEY = _RequestHolder("")


def _principal_from_claims(claims: Dict[str, Any]) -> Dict[str, Any]:
    groups = claims.get("cognito:groups") or []
    if isinstance(groups, str):
        groups = [groups]
    return {
        "kind": "cognito",
        "id": str(claims.get("email") or claims.get("username") or claims.get("sub") or "unknown"),
        "sub": str(claims.get("sub") or ""),
        "groups": [str(g) for g in groups],
        "allowed": bool(claims.get("sub")) and str(claims.get("sub")) in COGNITO_ALLOWED_SUBS,
    }


INTERNAL_PRINCIPAL = {"kind": "internal-key", "id": "internal-key", "sub": "", "groups": [], "allowed": True}


def _authenticate(event: Dict[str, Any]) -> tuple:
    """Validate bearer token. Returns (principal, None) or (None, error message)."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    auth = headers.get("authorization", "")

    if not auth.startswith("Bearer "):
        return None, "Missing or invalid Authorization header"

    token = auth[7:].strip()
    if not token:
        return None, "Empty bearer token"

    # Static API key check (internal key: full access)
    if MCP_API_KEY and hmac.compare_digest(token, MCP_API_KEY):
        return dict(INTERNAL_PRINCIPAL), None

    # Cognito JWT check (human principal; access governed by POLICY_TABLE)
    if COGNITO_USER_POOL_ID:
        try:
            return _principal_from_claims(_verify_cognito_jwt(token)), None
        except Exception as exc:
            return None, f"JWT validation failed: {exc}"

    return None, "No authentication method configured"


def _authenticate_request(event: Dict[str, Any]) -> Optional[str]:
    """Validate bearer token. Returns None on success, error message on failure."""
    return _authenticate(event)[1]


# ---------------------------------------------------------------------------
# Helper: server base URL
# ---------------------------------------------------------------------------

def _get_server_base_url(event: Dict[str, Any]) -> str:
    if SERVER_BASE_URL:
        return SERVER_BASE_URL.rstrip("/")
    headers = event.get("headers") or {}
    host = headers.get("host") or headers.get("Host") or "localhost"
    scheme = "https" if headers.get("x-forwarded-proto") == "https" else "https"
    return f"{scheme}://{host}"


# ---------------------------------------------------------------------------
# OAuth endpoint handlers (Cognito mode)
# ---------------------------------------------------------------------------

def _handle_oauth_metadata(event: Dict[str, Any]) -> Dict[str, Any]:
    base = _get_server_base_url(event)
    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json", "cache-control": "public, max-age=3600"},
        "body": json.dumps({
            "issuer": base,
            "authorization_endpoint": f"{base}/authorize",
            "token_endpoint": f"{base}/oauth/token",
            "registration_endpoint": f"{base}/oauth/register",
            "token_endpoint_auth_methods_supported": ["client_secret_post"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "response_types_supported": ["code"],
            "scopes_supported": ["openid", "email", "profile"],
            "code_challenge_methods_supported": ["S256"],
        }),
        "isBase64Encoded": False,
    }


def _handle_authorize(event: Dict[str, Any]) -> Dict[str, Any]:
    qs = event.get("queryStringParameters") or {}
    base = _get_server_base_url(event)
    cognito_domain = COGNITO_DOMAIN.rstrip("/")

    missing = [p for p in ("response_type", "client_id") if not qs.get(p)]
    if missing:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json", "cache-control": "no-store"},
            "body": json.dumps({
                "error": "invalid_request",
                "error_description": f"Missing required parameter(s): {', '.join(missing)}",
            }),
            "isBase64Encoded": False,
        }

    client_redirect_uri = qs.get("redirect_uri", "")
    client_state = qs.get("state", "")

    relay = json.dumps({"ru": client_redirect_uri, "st": client_state}, separators=(",", ":"))
    cognito_state = b64.urlsafe_b64encode(relay.encode()).decode().rstrip("=")

    params = {}
    for key in ("response_type", "client_id", "code_challenge", "code_challenge_method", "scope"):
        val = qs.get(key, "")
        if val:
            params[key] = val
    params["redirect_uri"] = f"{base}/callback"
    params["state"] = cognito_state
    if "scope" not in params:
        params["scope"] = "openid email profile"

    location = f"{cognito_domain}/oauth2/authorize?{urllib.parse.urlencode(params)}"
    return {
        "statusCode": 302,
        "headers": {"location": location, "cache-control": "no-store"},
        "body": "",
        "isBase64Encoded": False,
    }


def _handle_callback(event: Dict[str, Any]) -> Dict[str, Any]:
    qs = event.get("queryStringParameters") or {}
    code = qs.get("code", "")
    state_b64 = qs.get("state", "")

    if not code or not state_b64:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "invalid_request", "error_description": "Missing code or state"}),
            "isBase64Encoded": False,
        }

    # Decode relay state
    padding = 4 - len(state_b64) % 4
    if padding != 4:
        state_b64 += "=" * padding
    try:
        relay = json.loads(b64.urlsafe_b64decode(state_b64).decode())
    except Exception:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "invalid_state"}),
            "isBase64Encoded": False,
        }

    client_redirect = relay.get("ru", "")
    client_state = relay.get("st", "")

    if not client_redirect:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "missing_redirect_uri"}),
            "isBase64Encoded": False,
        }

    # Redirect back to client with the auth code
    sep = "&" if "?" in client_redirect else "?"
    location = f"{client_redirect}{sep}code={urllib.parse.quote(code)}"
    if client_state:
        location += f"&state={urllib.parse.quote(client_state)}"

    return {
        "statusCode": 302,
        "headers": {"location": location, "cache-control": "no-store"},
        "body": "",
        "isBase64Encoded": False,
    }


def _handle_token(event: Dict[str, Any]) -> Dict[str, Any]:
    cognito_domain = COGNITO_DOMAIN.rstrip("/")
    token_url = f"{cognito_domain}/oauth2/token"
    base = _get_server_base_url(event)

    body_raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body_raw = b64.b64decode(body_raw).decode()

    params = urllib.parse.parse_qs(body_raw, keep_blank_values=True)
    grant_type = (params.get("grant_type") or [""])[0]
    if not grant_type:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json", "cache-control": "no-store"},
            "body": json.dumps({"error": "invalid_request", "error_description": "Missing grant_type"}),
            "isBase64Encoded": False,
        }

    if "redirect_uri" in params:
        params["redirect_uri"] = [f"{base}/callback"]
        body_raw = urllib.parse.urlencode({k: v[0] for k, v in params.items()})

    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(certifi.where())
    except Exception:
        pass

    req = urllib.request.Request(
        token_url,
        data=body_raw.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            return {
                "statusCode": resp.status,
                "headers": {"content-type": "application/json", "cache-control": "no-store"},
                "body": resp.read().decode("utf-8"),
                "isBase64Encoded": False,
            }
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace") if exc.fp else "{}"
        return {
            "statusCode": exc.code,
            "headers": {"content-type": "application/json"},
            "body": err_body,
            "isBase64Encoded": False,
        }
    except Exception as exc:
        return {
            "statusCode": 502,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "server_error", "error_description": str(exc)}),
            "isBase64Encoded": False,
        }


def _handle_register(event: Dict[str, Any]) -> Dict[str, Any]:
    body_raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body_raw = b64.b64decode(body_raw).decode()
    try:
        reg = json.loads(body_raw) if body_raw else {}
    except Exception:
        reg = {}

    redirect_uris = reg.get("redirect_uris", [])
    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json", "cache-control": "no-store"},
        "body": json.dumps({
            "client_id": COGNITO_CLIENT_ID,
            "client_secret": COGNITO_CLIENT_SECRET,
            "redirect_uris": redirect_uris,
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "scope": "openid email profile",
            "token_endpoint_auth_method": "client_secret_post",
        }),
        "isBase64Encoded": False,
    }


def _handle_protected_resource(event: Dict[str, Any]) -> Dict[str, Any]:
    base = _get_server_base_url(event)
    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json", "cache-control": "public, max-age=3600"},
        "body": json.dumps({
            "resource": base,
            "authorization_servers": [base],
            "scopes_supported": ["openid", "email", "profile"],
        }),
        "isBase64Encoded": False,
    }


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _decimal_to_native(obj):
    """Convert DynamoDB Decimal types to Python native for JSON serialization."""
    if isinstance(obj, Decimal):
        if obj == int(obj):
            return int(obj)
        return float(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimal_to_native(i) for i in obj]
    return obj


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


MUTABLE_FIELDS = {
    "cost_type", "trip_city", "booked_class", "confirmation_code",
    "base_fare", "extra_fees", "total_paid", "funding_source",
    "flight_number", "website",
}

IMMUTABLE_FIELDS = {
    "flight_id", "date_iso", "date_yyyy_mm", "origin", "dest",
    "sequence", "source_year", "source_sheet", "status",
    "created_at", "international",
}


# ---------------------------------------------------------------------------
# Tool result helpers
# ---------------------------------------------------------------------------

def _success(data: Any) -> list:
    return [TextContent(type="text", text=json.dumps(
        {"success": True, "result": _decimal_to_native(data)}, indent=2, default=str
    ))]


def _error(code: str, message: str, status: int = 400, retryable: bool = False) -> list:
    return [TextContent(type="text", text=json.dumps({
        "success": False,
        "error": message,
        "error_envelope": {"code": code, "message": message, "retryable": retryable},
    }))]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

# DynamoDB's ``Limit`` bounds the items *evaluated*, before any FilterExpression
# runs, so one filtered call can return fewer rows than asked for -- even none --
# while matches remain (FLY-ISS-001).  _read_page keeps reading until it holds a
# full page or the key space is exhausted, within these bounds.
SEARCH_EVAL_CHUNK = 200    # items evaluated per DynamoDB call when a filter applies
SEARCH_MAX_DDB_CALLS = 20  # DynamoDB calls per tool call; once spent, a cursor carries on

# Key attributes of a resume cursor (ExclusiveStartKey), by access path.
_INDEX_KEY_ATTRS = {
    None: ("flight_id",),
    "route-index": ("flight_id", "origin", "dest"),
    "trip-city-index": ("flight_id", "trip_city", "date_iso"),
}


def _date_upper_bound(date_to: str) -> str:
    """A date-only upper bound means "through the end of that day"."""
    return f"{date_to}T23:59:59Z" if len(date_to) == 10 else date_to


def _build_search_query(args: dict) -> tuple:
    """Choose the access path; whatever the path cannot cover becomes a filter.

    Returns (kwargs, use_query) for table.query / table.scan, without Limit or
    ExclusiveStartKey.
    """
    date_from = args.get("date_from")
    date_to = _date_upper_bound(args["date_to"]) if args.get("date_to") else None
    origin = args.get("origin")
    dest = args.get("dest")
    trip_city = args.get("trip_city")

    kwargs: Dict[str, Any] = {}
    key_attrs: set = set()
    dates_in_key = False

    if origin and dest:
        kwargs["IndexName"] = "route-index"
        kwargs["KeyConditionExpression"] = Key("origin").eq(origin) & Key("dest").eq(dest)
        key_attrs = {"origin", "dest"}
    elif trip_city:
        kwargs["IndexName"] = "trip-city-index"
        kce = Key("trip_city").eq(trip_city)
        if date_from and date_to:
            kce = kce & Key("date_iso").between(date_from, date_to)
        elif date_from:
            kce = kce & Key("date_iso").gte(date_from)
        elif date_to:
            kce = kce & Key("date_iso").lte(date_to)
        kwargs["KeyConditionExpression"] = kce
        key_attrs = {"trip_city"}
        dates_in_key = True
    elif origin:
        kwargs["IndexName"] = "route-index"
        kwargs["KeyConditionExpression"] = Key("origin").eq(origin)
        key_attrs = {"origin"}
    # else: no usable key (dest alone, dates alone, or nothing) -> filtered scan

    filters = []
    for name, value in (("origin", origin), ("dest", dest), ("trip_city", trip_city)):
        if value and name not in key_attrs:
            filters.append(Attr(name).eq(value))
    if not dates_in_key:
        if date_from and date_to:
            filters.append(Attr("date_iso").between(date_from, date_to))
        elif date_from:
            filters.append(Attr("date_iso").gte(date_from))
        elif date_to:
            filters.append(Attr("date_iso").lte(date_to))
    for name in ("airline", "booked_class", "status", "cost_type"):
        if args.get(name):
            filters.append(Attr(name).eq(args[name]))
    if args.get("international") is not None:
        filters.append(Attr("international").eq(bool(args["international"])))

    if filters:
        expr = filters[0]
        for extra in filters[1:]:
            expr = expr & extra
        kwargs["FilterExpression"] = expr

    return kwargs, "IndexName" in kwargs


def _item_key(item: dict, index_name: Optional[str]) -> dict:
    """The ExclusiveStartKey that resumes immediately after ``item``."""
    return {attr: item[attr] for attr in _INDEX_KEY_ATTRS[index_name]}


def _read_page(table, kwargs: dict, use_query: bool, limit: int, start_key: Optional[dict]) -> tuple:
    """Read up to ``limit`` matching items; returns (items, resume_key).

    resume_key is None only when the search is exhausted.  It is also returned,
    possibly with a short page, if the DynamoDB call budget runs out first.
    """
    read = table.query if use_query else table.scan
    index_name = kwargs.get("IndexName")
    filtered = "FilterExpression" in kwargs
    items: List[dict] = []
    resume = start_key
    for _ in range(SEARCH_MAX_DDB_CALLS):
        call_kwargs = dict(kwargs)
        # Unfiltered, every evaluated item is returned, so one extra item tells
        # us whether a next page exists without issuing a cursor to an empty one.
        call_kwargs["Limit"] = SEARCH_EVAL_CHUNK if filtered else limit + 1
        if resume:
            call_kwargs["ExclusiveStartKey"] = resume
        response = read(**call_kwargs)
        items.extend(response.get("Items", []))
        resume = response.get("LastEvaluatedKey")
        if len(items) > limit:
            # A further match exists: resume right after the last item returned.
            return items[:limit], _item_key(items[limit - 1], index_name)
        if not resume:
            return items, None
    return items, resume


async def _search_flights(args: dict) -> list:
    """Search flights with optional filters and pagination."""
    table = _get_table()
    limit = max(1, min(int(args.get("limit", 50)), 50))

    start_key = None
    if args.get("next_token"):
        try:
            start_key = json.loads(b64.b64decode(args["next_token"]).decode())
        except ValueError:
            start_key = None
        if not isinstance(start_key, dict):
            return _error("invalid_next_token", "next_token is not a cursor issued by search_flights")

    kwargs, use_query = _build_search_query(args)
    try:
        items, last_key = _read_page(table, kwargs, use_query, limit, start_key)
    except ClientError as exc:
        if start_key and exc.response.get("Error", {}).get("Code") == "ValidationException":
            return _error(
                "invalid_next_token",
                "next_token does not belong to this search; repeat the search without it",
            )
        raise

    # Build summary records
    summaries = []
    for item in items:
        summaries.append({
            "flight_id": item.get("flight_id"),
            "date_iso": item.get("date_iso"),
            "origin": item.get("origin"),
            "dest": item.get("dest"),
            "trip_city": item.get("trip_city"),
            "airline": item.get("airline"),
            "status": item.get("status"),
            "total_paid": item.get("total_paid"),
            "booked_class": item.get("booked_class"),
            "cost_type": item.get("cost_type"),
        })

    result: Dict[str, Any] = {"flights": summaries, "count": len(summaries)}

    if last_key:
        result["next_token"] = b64.b64encode(json.dumps(
            _decimal_to_native(last_key), default=str
        ).encode()).decode()

    return _success(result)


async def _get_flight(args: dict) -> list:
    """Get a single flight by ID."""
    flight_id = args.get("flight_id")
    if not flight_id:
        return _error("missing_param", "flight_id is required")

    table = _get_table()
    response = table.get_item(Key={"flight_id": flight_id}, ConsistentRead=True)
    item = response.get("Item")
    if not item:
        return _error("not_found", f"Flight {flight_id} not found", status=404)

    return _success(item)


async def _update_flight(args: dict) -> list:
    """Update mutable fields on a flight record."""
    flight_id = args.get("flight_id")
    if not flight_id:
        return _error("missing_param", "flight_id is required")

    updates = {k: v for k, v in args.items() if k != "flight_id"}
    if not updates:
        return _error("no_updates", "No fields provided to update")

    # Reject immutable field changes
    immutable_attempted = set(updates.keys()) & IMMUTABLE_FIELDS
    if immutable_attempted:
        return _error(
            "immutable_field",
            f"Cannot modify immutable field(s): {', '.join(sorted(immutable_attempted))}",
        )

    # Reject unknown fields
    unknown = set(updates.keys()) - MUTABLE_FIELDS
    if unknown:
        return _error(
            "unknown_field",
            f"Unknown field(s): {', '.join(sorted(unknown))}. Mutable fields: {', '.join(sorted(MUTABLE_FIELDS))}",
        )

    table = _get_table()

    # Verify flight exists
    existing = table.get_item(Key={"flight_id": flight_id}, ConsistentRead=True)
    if not existing.get("Item"):
        return _error("not_found", f"Flight {flight_id} not found", status=404)

    # Build update expression
    expr_parts = ["#updated_at = :updated_at"]
    attr_names = {"#updated_at": "updated_at"}
    attr_values: Dict[str, Any] = {":updated_at": _now_iso()}

    for i, (field, value) in enumerate(updates.items()):
        placeholder_name = f"#f{i}"
        placeholder_val = f":v{i}"
        expr_parts.append(f"{placeholder_name} = {placeholder_val}")
        attr_names[placeholder_name] = field
        # Convert float values to Decimal for DynamoDB
        if isinstance(value, float):
            attr_values[placeholder_val] = Decimal(str(round(value, 2)))
        else:
            attr_values[placeholder_val] = value

    response = table.update_item(
        Key={"flight_id": flight_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
        ReturnValues="ALL_NEW",
    )

    return _success(response["Attributes"])


async def _cancel_flight(args: dict) -> list:
    """Cancel a flight (soft delete — sets status to 'cancelled')."""
    flight_id = args.get("flight_id")
    if not flight_id:
        return _error("missing_param", "flight_id is required")

    table = _get_table()

    # Verify flight exists
    existing = table.get_item(Key={"flight_id": flight_id}, ConsistentRead=True)
    item = existing.get("Item")
    if not item:
        return _error("not_found", f"Flight {flight_id} not found", status=404)

    # Idempotent: already cancelled
    if item.get("status") == "cancelled":
        return _success(item)

    now = _now_iso()
    response = table.update_item(
        Key={"flight_id": flight_id},
        UpdateExpression="SET #status = :status, #cancelled_at = :cancelled_at, #updated_at = :updated_at",
        ExpressionAttributeNames={
            "#status": "status",
            "#cancelled_at": "cancelled_at",
            "#updated_at": "updated_at",
        },
        ExpressionAttributeValues={
            ":status": "cancelled",
            ":cancelled_at": now,
            ":updated_at": now,
        },
        ReturnValues="ALL_NEW",
    )

    return _success(response["Attributes"])


async def _create_flight(args: dict) -> list:
    """Create a new flight record."""
    # Enforce required fields
    date_str = args.get("date")
    origin = args.get("origin")
    dest = args.get("dest")

    missing = []
    if not date_str:
        missing.append("date")
    if not origin:
        missing.append("origin")
    if not dest:
        missing.append("dest")
    if missing:
        return _error("missing_param", f"Required field(s) missing: {', '.join(missing)}")

    # Parse and validate date
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        date_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_yyyy_mm = dt.strftime("%Y-%m")
    except (ValueError, AttributeError):
        return _error("invalid_date", f"Invalid date format: {date_str}. Use ISO 8601 (e.g. 2025-06-15).")

    # Deterministic flight_id (row_index=0 for manually created records)
    seq = str(args.get("sequence", "0")) if args.get("sequence") is not None else "0"
    flight_id = hashlib.sha256(f"{date_iso}|{origin}|{dest}|{seq}|0".encode()).hexdigest()[:12]

    now = _now_iso()
    item: Dict[str, Any] = {
        "flight_id": flight_id,
        "date_iso": date_iso,
        "date_yyyy_mm": date_yyyy_mm,
        "origin": origin.upper(),
        "dest": dest.upper(),
        "status": "active",
        "created_at": now,
        "updated_at": now,
    }

    # Optional fields
    OPTIONAL_FIELDS = {
        "cost_type": str, "trip_city": str, "airline": str,
        "booked_class": str, "international": bool,
        "eqm_miles": Decimal, "base_fare": Decimal,
        "extra_fees": Decimal, "total_paid": Decimal,
        "funding_source": str, "confirmation_code": str,
        "flight_number": str, "website": str, "sequence": Decimal,
    }

    for field, field_type in OPTIONAL_FIELDS.items():
        val = args.get(field)
        if val is not None:
            if field_type == Decimal:
                item[field] = Decimal(str(round(float(val), 2)))
            elif field_type == bool:
                item[field] = bool(val)
            else:
                item[field] = str(val)

    table = _get_table()

    # Reject duplicates with ConditionExpression
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(flight_id)",
        )
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return _error(
            "duplicate",
            f"Flight {flight_id} already exists (same date/origin/dest/sequence). Use update_flight to modify it.",
            status=409,
        )

    return _success(item)


# ---------------------------------------------------------------------------
# MCP Server setup
# ---------------------------------------------------------------------------

app = Server("io-travel")

_TOOL_HANDLERS = {
    "search_flights": _search_flights,
    "get_flight": _get_flight,
    "create_flight": _create_flight,
    "update_flight": _update_flight,
    "cancel_flight": _cancel_flight,
}


def _base_tools() -> list[Tool]:
    return [
        Tool(
            name="search_flights",
            description="Search flight records with optional filters. Returns paginated summaries.",
            inputSchema={
                "type": "object",
                "properties": {
                    "date_from": {"type": "string", "description": "Start date (ISO 8601, e.g. 2025-01-01)"},
                    "date_to": {"type": "string", "description": "End date (ISO 8601); a date-only value includes that whole day"},
                    "origin": {"type": "string", "description": "Origin airport code (e.g. SEA)"},
                    "dest": {"type": "string", "description": "Destination airport code (e.g. SFO)"},
                    "airline": {"type": "string", "description": "Airline name (Alaska, Partner)"},
                    "booked_class": {"type": "string", "description": "Booking class (First, Economy, K, D, Business)"},
                    "trip_city": {"type": "string", "description": "Trip city name"},
                    "status": {"type": "string", "description": "Flight status (active, cancelled)"},
                    "cost_type": {"type": "string", "description": "Cost type (Air, Car, Airfare, Hotel)"},
                    "international": {"type": "boolean", "description": "International flight filter"},
                    "limit": {"type": "integer", "description": "Max results (1-50, default 50)"},
                    "next_token": {"type": "string", "description": "Pagination cursor from previous response; absent means no more results"},
                },
            },
        ),
        Tool(
            name="get_flight",
            description="Get full details of a single flight by its ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "flight_id": {"type": "string", "description": "The flight record ID"},
                },
                "required": ["flight_id"],
            },
        ),
        Tool(
            name="create_flight",
            description="Create a new flight record. Requires date, origin, and destination airport codes. Generates a unique flight ID. Rejects duplicates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "Flight date (ISO 8601, e.g. 2025-06-15 or 2025-06-15T08:30:00Z)"},
                    "origin": {"type": "string", "description": "Origin airport code (e.g. SEA)"},
                    "dest": {"type": "string", "description": "Destination airport code (e.g. SFO)"},
                    "cost_type": {"type": "string", "description": "Cost type (Air, Car, Airfare, Hotel)"},
                    "trip_city": {"type": "string", "description": "Trip city name"},
                    "airline": {"type": "string", "description": "Airline name (e.g. Alaska)"},
                    "booked_class": {"type": "string", "description": "Booking class (First, Economy, Business, etc.)"},
                    "international": {"type": "boolean", "description": "International flight flag"},
                    "eqm_miles": {"type": "number", "description": "Elite qualifying miles"},
                    "base_fare": {"type": "number", "description": "Base fare amount"},
                    "extra_fees": {"type": "number", "description": "Extra fees amount"},
                    "total_paid": {"type": "number", "description": "Total paid amount"},
                    "funding_source": {"type": "string", "description": "Payment source (e.g. AMEX 0308)"},
                    "confirmation_code": {"type": "string", "description": "Booking confirmation code"},
                    "flight_number": {"type": "string", "description": "Flight number (e.g. AS123)"},
                    "website": {"type": "string", "description": "Booking website"},
                    "sequence": {"type": "number", "description": "Trip leg sequence number"},
                },
                "required": ["date", "origin", "dest"],
            },
        ),
        Tool(
            name="update_flight",
            description="Update mutable booking fields on a flight record. Immutable fields (date, origin, dest, status) cannot be changed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "flight_id": {"type": "string", "description": "The flight record ID to update"},
                    "cost_type": {"type": "string"},
                    "trip_city": {"type": "string"},
                    "booked_class": {"type": "string"},
                    "confirmation_code": {"type": "string"},
                    "base_fare": {"type": "number"},
                    "extra_fees": {"type": "number"},
                    "total_paid": {"type": "number"},
                    "funding_source": {"type": "string"},
                    "flight_number": {"type": "string"},
                    "website": {"type": "string"},
                },
                "required": ["flight_id"],
            },
        ),
        Tool(
            name="cancel_flight",
            description="Cancel a flight reservation. Sets status to 'cancelled' with a timestamp. Idempotent — re-cancelling returns success. No hard deletes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "flight_id": {"type": "string", "description": "The flight record ID to cancel"},
                },
                "required": ["flight_id"],
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Capabilities (caps.json), annotations, x-io-minted, policy table (DVP-TSK-889)
# ---------------------------------------------------------------------------

WRITE_TOOLS = ("create_flight", "update_flight", "cancel_flight")

_ANNOTATIONS = {
    "search_flights": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
    "get_flight": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
    "create_flight": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False),
    "update_flight": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
    "cancel_flight": dict(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False),
}

# JSON Pointers into the tool result (the {"success","result"} envelope) minted server-side.
_X_IO_MINTED = {"create_flight": ["/result/flight_id", "/result/created_at"]}

# Per-tool authorisation: read tools need any valid principal, write tools need
# the internal key or a Cognito group in TRAVEL_WRITE_GROUPS.
POLICY_TABLE = {
    name: {"access": "write" if name in WRITE_TOOLS else "read"} for name in _ANNOTATIONS
}


def _schema_hash(schema: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _tool_specs() -> list[Tool]:
    tools = []
    for t in _base_tools():
        meta = {}
        if t.name in _X_IO_MINTED:
            meta["x-io-minted"] = list(_X_IO_MINTED[t.name])
        if t.name in _X_IO_MINTED:
            schema = json.loads(json.dumps(t.inputSchema))
            for ptr in _X_IO_MINTED[t.name]:
                schema["properties"][ptr.rsplit("/", 1)[-1]] = {
                    "type": "string", "x-io-minted": True,
                    "description": "Server-minted; read-only, ignored if supplied",
                }
            t = t.model_copy(update={"inputSchema": schema})
        tools.append(t.model_copy(update={
            "annotations": ToolAnnotations(**_ANNOTATIONS[t.name]),
            "meta": meta or None,
        }))
    return tools


_DRY_RUN_TOOLS: tuple = ()  # tools whose server-side dry_run is implemented (DVP-TSK-900)


def build_caps() -> Dict[str, Any]:
    """The io-travel capabilities document: parity caps.schema 1.1.0 (orchestrator ruling E2-R11)."""
    actions = []
    for t in _tool_specs():
        actions.append({
            "name": t.name,
            "title": t.name.replace("_", " ").title(),
            "inputSchema": t.inputSchema,
            "outputSchema": None,
            "annotations": dict(_ANNOTATIONS[t.name]),
            "requiresGovernanceHash": False,
            "via": "top-level",
            "dryRun": t.name in _DRY_RUN_TOOLS,
        })
    return {"schemaVersion": "1.1.0", "surface": "io-travel", "generatedAt": _now_iso(), "actions": actions}


def build_permission_manifest() -> Dict[str, Any]:
    return {
        "v": 1,
        "surface": "io-travel",
        "principals": {
            "internal-key": {"access": "all"},
            "cognito": {
                "default": "deny",
                "allow_list_env": "COGNITO_ALLOWED_SUBS",
                "applies_to": ["read", "write"],
                "write_groups_env": "TRAVEL_WRITE_GROUPS",
                "write_groups": list(TRAVEL_WRITE_GROUPS),
            },
        },
        "tools": {name: dict(rule) for name, rule in POLICY_TABLE.items()},
    }


def _authorize(principal: Optional[Dict[str, Any]], tool: str) -> Optional[str]:
    """Return None if allowed, else a refusal message."""
    rule = POLICY_TABLE.get(tool)
    if rule is None or principal is None:
        return "No principal or unknown tool"
    if principal.get("kind") == "internal-key":
        return None
    if not principal.get("allowed"):
        return "principal is not in the COGNITO_ALLOWED_SUBS allow-list"
    if rule["access"] == "write" and TRAVEL_WRITE_GROUPS and not (
        set(principal.get("groups") or []) & set(TRAVEL_WRITE_GROUPS)
    ):
        return f"{tool} requires membership of one of: {', '.join(TRAVEL_WRITE_GROUPS)}"
    return None


@app.list_tools()
async def list_tools() -> list[Tool]:
    return _tool_specs()


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    handler = _TOOL_HANDLERS.get(name)
    if handler is None:
        return _error("unknown_tool", f"Unknown tool: {name}")
    principal = _PRINCIPAL.get()
    denied = _authorize(principal, name)
    audit = {
        "audit": "travel_tool_call", "tool": name,
        "principal": (principal or {}).get("id"), "principal_kind": (principal or {}).get("kind"),
        "idempotency_key": _IDEMPOTENCY_KEY.get() or None,
        "allowed": denied is None, "ts": _now_iso(),
    }
    logger.info(json.dumps(audit))
    if denied:
        return _error("forbidden", denied, status=403)
    try:
        return await handler(arguments or {})
    except Exception as exc:
        logger.exception(f"Tool {name} failed")
        return _error("internal_error", str(exc), status=500, retryable=True)


# ---------------------------------------------------------------------------
# Lambda handler + ASGI adapter
# ---------------------------------------------------------------------------

_http_session_manager = None


def _get_http_session_manager():
    global _http_session_manager
    if _http_session_manager is None:
        from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
        _http_session_manager = StreamableHTTPSessionManager(
            app=app, json_response=True, stateless=True,
        )
    return _http_session_manager


def _json_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(doc),
        "isBase64Encoded": False,
    }


async def _handle_lambda_event(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return await _handle_lambda_event_inner(event)
    finally:
        _PRINCIPAL.reset()
        _IDEMPOTENCY_KEY.reset()


async def _handle_lambda_event_inner(event: Dict[str, Any]) -> Dict[str, Any]:
    req_ctx = event.get("requestContext", {}).get("http", {})
    method = req_ctx.get("method", event.get("httpMethod", "POST")).upper()
    path = req_ctx.get("path", event.get("rawPath", "/"))

    # Strip stage prefix if present
    if path.startswith("/default"):
        path = path[len("/default"):]
    if not path:
        path = "/"

    # OAuth / well-known routes (no auth required)
    if path == "/.well-known/oauth-authorization-server" and method == "GET":
        return _handle_oauth_metadata(event)
    if path == "/.well-known/oauth-protected-resource" and method == "GET":
        return _handle_protected_resource(event)
    if path == "/authorize" and method == "GET":
        return _handle_authorize(event)
    if path == "/callback" and method == "GET":
        return _handle_callback(event)
    if path == "/oauth/token" and method == "POST":
        return _handle_token(event)
    if path == "/oauth/register" and method == "POST":
        return _handle_register(event)

    # MCP routes require authentication
    # Every route below the OAuth/well-known ones is an MCP or caps route and needs auth.
    # The claude.ai connector POSTs MCP to "/" (and "/default" strips to "/"), so gating
    # only /mcp left the tool layer without a principal (DVP-ISS-144).
    if True:
        principal, auth_err = _authenticate(event)
        if not auth_err and not principal.get("allowed"):
            return {
                "statusCode": 403,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"error": "forbidden", "error_description": "principal is not in the allow-list"}),
                "isBase64Encoded": False,
            }
        if not auth_err:
            _PRINCIPAL.set(principal)
            _hdrs = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
            _IDEMPOTENCY_KEY.set(_hdrs.get("idempotency-key", ""))
            if path == "/caps.json" and method == "GET":
                return _json_response(build_caps())
            if path == "/permission-manifest.json" and method == "GET":
                return _json_response(build_permission_manifest())
        if auth_err:
            return {
                "statusCode": 401,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"error": "unauthorized", "error_description": auth_err}),
                "isBase64Encoded": False,
            }

    # Build ASGI scope
    raw_headers = event.get("headers") or {}
    asgi_headers = [(k.lower().encode(), v.encode()) for k, v in raw_headers.items()]

    body_raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body_bytes = b64.b64decode(body_raw)
    else:
        body_bytes = body_raw.encode("utf-8") if isinstance(body_raw, str) else body_raw

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": event.get("rawPath", path).encode(),
        "query_string": event.get("rawQueryString", "").encode(),
        "root_path": "",
        "scheme": "https",
        "server": ("lambda", 443),
        "headers": asgi_headers,
    }

    body_consumed = False

    async def receive():
        nonlocal body_consumed
        if not body_consumed:
            body_consumed = True
            return {"type": "http.request", "body": body_bytes, "more_body": False}
        await anyio.sleep_forever()

    resp_status = 200
    resp_headers: Dict[str, str] = {}
    resp_body = b""

    async def send(message):
        nonlocal resp_status, resp_headers, resp_body
        if message["type"] == "http.response.start":
            resp_status = int(message["status"])
            for k, v in message.get("headers", []):
                resp_headers[k.decode()] = v.decode()
        elif message["type"] == "http.response.body":
            resp_body += message.get("body", b"")

    manager = _get_http_session_manager()
    async with anyio.create_task_group() as tg:
        manager._task_group = tg
        manager._has_started = True
        await manager.handle_request(scope, receive, send)
        tg.cancel_scope.cancel()

    return {
        "statusCode": resp_status,
        "headers": resp_headers,
        "body": resp_body.decode("utf-8", errors="replace"),
        "isBase64Encoded": False,
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if MCP_TRANSPORT != "streamable_http":
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Requires MCP_TRANSPORT=streamable_http"}),
            "isBase64Encoded": False,
        }
    return asyncio.run(_handle_lambda_event(event))
