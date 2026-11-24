#!/usr/bin/env python3
"""Resolve the Lambda environment for deploy.sh (E2-R12).

Merges the function's CURRENT environment (JSON on stdin or $CURRENT_ENV) with
non-empty values supplied by the deploy shell, so unspecified variables -- notably
COGNITO_ALLOWED_SUBS -- are preserved.  Exits 3 (printing nothing on stdout) when a
required setting (contract.SETTINGS) is absent from the resolved environment, or a Cognito pool is
configured but COGNITO_ALLOWED_SUBS is empty.
Values are never printed except in the resolved JSON on stdout.
"""
import json
import os
import sys

import contract
import door
import settings

# Every setting a deployment supplies is declared by the contract; nothing here names a deployment.
KEYS = list(contract.SETTINGS)

# ENC-ISS-919: io-kit ships no fleet registry, so the public edge's connector comes from the deployment. A setting the
# function reads (IO_KIT_FLEET_JSON, inline JSON), not named by the contract; only the public door needs one.
FLEET_KEYS = ("IO_KIT_FLEET_JSON",)

# FLY-TSK-083: the read-only /records routes accept a human-door token only. Read by records_door, not named by the
# contract (like the fleet registry); absent means the routes refuse every request.
RECORDS_DOOR_KEYS = ("RECORDS_DOOR_ISSUERS", "RECORDS_DOOR_AUDIENCE", "RECORDS_DOOR_PUBLIC_KEYS")

# Deploy-time settings the deploy script and workflow need (they never reach the function). Plan refuses
# when one is absent, so a missing one stops the run before apply.
DEPLOY_SETTINGS = ("FUNCTION_NAME", "AWS_REGION", "CODEARTIFACT_DOMAIN", "CODEARTIFACT_OWNER",
                   "CODEARTIFACT_REGION", "CODEARTIFACT_REPOSITORY", "PROBE_MCP_URL", "PROBE_SIGNIN_URL")
# N6F (F-10): an internal deployment has no address to probe; its check is the health read. The two probe
# addresses are required only when the door is public, and a missing or unknown door reads as public.
PUBLIC_ONLY_DEPLOY_SETTINGS = ("PROBE_MCP_URL", "PROBE_SIGNIN_URL")

# FLY-TSK-022: retired by the edge adoption. Stripped from the function env on every deploy so a
# stale confidential-client secret or static key can never survive the merge with CURRENT_ENV.
DROPPED = ("FINANCE_READ_CREDENTIAL", "FINANCE_READ_CREDENTIAL_SECRET",  # FLY-TSK-049/078: the bearer path is retired; stripped from the live function on deploy
           "COGNITO_CLIENT_SECRET", "COGNITO_DOMAIN", "SERVER_BASE_URL", "MCP_API_KEY",
           "IOGRAPH_POOL_ID", "IOGRAPH_REGION", "IOGRAPH_CLIENT_IDS")


def resolve(current: dict, supplied: dict) -> dict:
    env = dict(current or {})
    env["MCP_TRANSPORT"] = "streamable_http"
    for k in KEYS + list(door.DOOR_KEYS) + list(FLEET_KEYS) + list(RECORDS_DOOR_KEYS):
        if supplied.get(k):
            env[k] = supplied[k]
    return {k: v for k, v in env.items() if v and k not in DROPPED}


def guard(env: dict) -> str:
    """Non-empty message when the resolved environment must not be applied (FLY-TSK-028-0B, A-2).

    Fail closed: any required contract setting absent from the resolved env stops the deploy
    before apply. Names KEYS only. The Cognito allow-list guard below is preserved.
    """
    if env.get("COGNITO_USER_POOL_ID") and not (env.get("COGNITO_ALLOWED_SUBS") or "").strip(" ,"):
        return "refusing to deploy: COGNITO_USER_POOL_ID is set but COGNITO_ALLOWED_SUBS is empty (would lock out the Cognito path)"
    if env.get("FINANCE_READ_FUNCTION") and settings.finance_function_problem(env["FINANCE_READ_FUNCTION"]):
        return "refusing to deploy: FINANCE_READ_FUNCTION must be a Lambda function name or ARN, not a URL"
    missing = settings.missing_required(env)
    if missing:
        return "refusing to deploy: required settings absent: " + ", ".join(sorted(missing))
    try:
        door.load(env)
    except door.DoorError as exc:
        return "refusing to deploy: " + str(exc).replace("refusing to start: ", "")
    return ""


def fleet_problem(env: dict) -> str:
    """Non-empty message when a public edge is resolved without its connector registry (ENC-ISS-919). The internal door
    resolves no connector, so it needs none (and hers must carry none)."""
    if (env.get("DOOR_MODE") or "").strip() != "internal" and not (env.get("IO_KIT_FLEET_JSON") or "").strip():
        return "refusing to deploy: a public edge needs IO_KIT_FLEET_JSON (io-kit ships no fleet registry)"
    return ""


def missing_deploy_settings(environ=None) -> list:
    environ = os.environ if environ is None else environ
    internal = (environ.get("DOOR_MODE") or "").strip() == "internal"
    return [k for k in DEPLOY_SETTINGS
            if not (internal and k in PUBLIC_ONLY_DEPLOY_SETTINGS) and not (environ.get(k) or "").strip()]


def diff_env(current: dict, resolved: dict) -> list:
    """Key-level plan of an environment change: [(key, 'added'|'removed'|'changed')].

    Reports KEYS ONLY; values are never returned, so the result is safe to print.
    """
    current, resolved = current or {}, resolved or {}
    out = []
    for k in sorted(set(current) | set(resolved)):
        if k not in current:
            out.append((k, "added"))
        elif k not in resolved:
            out.append((k, "removed"))
        elif current[k] != resolved[k]:
            out.append((k, "changed"))
    return out


def main() -> int:
    raw = os.environ.get("CURRENT_ENV", "") or "{}"
    try:
        current = json.loads(raw)
        if current is None:
            current = {}
    except ValueError:
        current = {}
    env = resolve(current, os.environ)
    msg = guard(env) or fleet_problem(env)
    if msg:
        print(msg, file=sys.stderr)
        return 3
    if "--plan" in sys.argv[1:]:
        # Dry-run: print key + marker only (never values); change nothing.
        for k, marker in diff_env(current, env):
            print(f"  {marker:8s} {k}")
        return 0
    print(json.dumps({"Variables": env}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
