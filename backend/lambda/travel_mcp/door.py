"""Which door a deployment answers through, and the refusal of the other one (FLY-TSK-028-0D, A-4).

A deployment is exactly one of two things, named by the DOOR_MODE setting:
  public    reachable through a Function URL. Answers HTTP-shaped events (requestContext.http) only;
            refuses anything else (mirror of finance's NonHttpEventRefused).
  internal  reachable only by direct invocation. Answers a direct event from the role named by
            INTERNAL_INVOKER_ROLE; refuses any HTTP-shaped event.
No default: an absent or unknown DOOR_MODE refuses to start, like every other setting (lane 1A).
Lambda does not attest the caller inside the event, so the role is a declared claim checked against the
setting; the enforcing gate is the function's resource policy, which the template grants to nobody
publicly when the deployment is internal. Messages name keys, never values. Stdlib only.

IMPORTANT: `invoker_role` is a DECLARED CLAIM, not an attested identity. Lambda does not tell the function
who invoked it, so anyone able to invoke at all can write any role string. The claim check is a
misconfiguration tripwire (defence in depth), NOT an authorization boundary: the only real gate is the
function's resource policy (the template's InvokeFunction grants, scoped to the named roles). A test that
a wrong claim is refused proves the tripwire, never isolation between deployments.

Health read (FLY-TSK-028-0D, BRD LD-39): the JSON-RPC method "ping" is answered by door.health() and never
reaches a tool, a table or the network: read-only, no person's data. The role named by INTERNAL_DEPLOY_ROLE
(optional) is answered that read and nothing else; INTERNAL_INVOKER_ROLE gets the tools as well.

Direct event shape: {"invoker_role": "<role arn>", "mcp": {<one JSON-RPC request>},
                     "acting_principal": "<id>", "session": "<id>"}   (FLY-TSK-032-0B, A2 of DOC-14994CDDA217)
`acting_principal` and `session` are optional. Each is a string matching ID_PATTERN; a principal that equals a role named in
settings is absent, and any other value counts as absent too (never an error). The acting person is an UNVERIFIED LABEL for
the author stamp: admission (admit) never reads it, and the only test applied to it anywhere is that it is present. They are
read here, only at the top level of an admitted direct event, and nowhere on the public edge.
"""
import json
import os
import re

import settings

MODE_KEY = "DOOR_MODE"
ROLE_KEY = "INTERNAL_INVOKER_ROLE"
MODES = ("public", "internal")
DEPLOY_ROLE_KEY = "INTERNAL_DEPLOY_ROLE"
HEALTH_METHOD = "ping"
DOOR_KEYS = (MODE_KEY, ROLE_KEY, DEPLOY_ROLE_KEY)


class DoorError(settings.SettingsError):
    """DOOR_MODE (or the role it needs) is absent or invalid; the function must not start."""


class NonHttpEventRefused(Exception):
    """A public-edge deployment was handed an event that did not come through its Function URL."""


class HttpEventRefused(Exception):
    """An internal-door deployment was handed an HTTP-shaped event."""


class InvokerRefused(Exception):
    """A direct event did not name the configured invoker role, or carried no request."""


def load(environ=None):
    """(mode, invoker_role). Raises DoorError when the mode is absent/invalid or internal has no role."""
    env = os.environ if environ is None else environ
    mode = str(env.get(MODE_KEY) or "").strip()
    if mode not in MODES:
        raise DoorError(f"refusing to start: {MODE_KEY} must be one of {', '.join(MODES)}")
    role = str(env.get(ROLE_KEY) or "").strip()
    if mode == "internal" and not role:
        raise DoorError(f"refusing to start: {ROLE_KEY} is required when {MODE_KEY} is internal")
    return mode, role


def deploy_role(environ=None) -> str:
    env = os.environ if environ is None else environ
    return str(env.get(DEPLOY_ROLE_KEY) or "").strip()


def is_http_shaped(event) -> bool:
    rc = event.get("requestContext") if isinstance(event, dict) else None
    return isinstance(rc, dict) and bool(rc.get("http"))


def admit(event, mode, role, deploy=""):
    """Return "http", "direct" or "health" when the event may be served in this mode; raise a refusal otherwise."""
    http = is_http_shaped(event)
    if mode == "public":
        if not http:
            raise NonHttpEventRefused("public edge refuses an event that did not come through its Function URL")
        return "http"
    if http:
        raise HttpEventRefused("internal door refuses an HTTP-shaped event")
    claimed, mcp = event.get("invoker_role"), event.get("mcp")
    if (not isinstance(mcp, dict) or not isinstance(claimed, str) or not claimed
            or claimed not in {r for r in (role, deploy) if r}):  # a non-string claim is refused, never raised on
        raise InvokerRefused("internal door answers only a direct event from a named role")
    if mcp.get("method") == HEALTH_METHOD:
        return "health"
    if claimed != role:  # the deploy role is answered the health read and nothing else
        raise InvokerRefused("the deploy role is answered the health read only")
    return "direct"


def health(event, surface) -> dict:
    """The one health read: alive and answering through the internal door. No I/O, no person's data."""
    rid = (event.get("mcp") or {}).get("id")
    return {"jsonrpc": "2.0", "id": rid, "result": {"status": "ok", "door": "internal", "surface": surface}}


def refusal(exc) -> dict:
    """The response for a refused event: 403 in the HTTP response shape, no detail beyond the class."""
    return {"statusCode": 403, "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "refused", "reason": type(exc).__name__}), "isBase64Encoded": False}


ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}")


def _opaque_id(value, roles=()) -> str:
    """The value when it is a string matching ID_PATTERN and not a named role; otherwise "" (absent, not an error)."""
    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None or value in roles:
        return ""
    return value


def acting(event, role, deploy="") -> tuple:
    """(acting_principal, session) from the top level of an ADMITTED direct event; "" for each that is absent or invalid.
    Called only for the internal door's "direct" shape. Never consulted by admit()."""
    roles = {r for r in (role, deploy) if r}
    return _opaque_id(event.get("acting_principal"), roles), _opaque_id(event.get("session"))


def direct_to_http(event, role, deploy="") -> dict:
    """Wrap an admitted direct event as the request the tool handler already serves, carrying the role as principal
    and, as unverified labels, the acting person and the session (empty strings when absent)."""
    person, session = acting(event, role, deploy)
    return {
        "requestContext": {"http": {"method": "POST", "path": "/mcp"}},
        "rawPath": "/mcp",
        "headers": {"content-type": "application/json", "accept": "application/json, text/event-stream"},
        "body": json.dumps(event["mcp"]),
        "isBase64Encoded": False,
        "io_kit_mcp": {"principal": {"sub": role, "groups": [], "acting_principal": person, "session": session},
                        "pool": "internal-door"},
    }
