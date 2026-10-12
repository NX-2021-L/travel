#!/usr/bin/env python3
"""Resolve the Lambda environment for deploy.sh (E2-R12).

Merges the function's CURRENT environment (JSON on stdin or $CURRENT_ENV) with
non-empty values supplied by the deploy shell, so unspecified variables -- notably
COGNITO_ALLOWED_SUBS -- are preserved.  Exits 3 (printing nothing on stdout) when a
Cognito pool is configured but the resolved COGNITO_ALLOWED_SUBS is empty.
Values are never printed except in the resolved JSON on stdout.
"""
import json
import os
import sys

KEYS = [
    "TABLE_NAME", "COGNITO_USER_POOL_ID", "COGNITO_CLIENT_ID", "COGNITO_REGION", "COGNITO_ALLOWED_SUBS",
    "IOGRAPH_ALLOWED_SUBS", "TRAVEL_WRITE_GROUPS",
]

# FLY-TSK-022: retired by the edge adoption. Stripped from the function env on every deploy so a
# stale confidential-client secret or static key can never survive the merge with CURRENT_ENV.
DROPPED = ("COGNITO_CLIENT_SECRET", "COGNITO_DOMAIN", "SERVER_BASE_URL", "MCP_API_KEY",
           "IOGRAPH_POOL_ID", "IOGRAPH_REGION", "IOGRAPH_CLIENT_IDS")


def resolve(current: dict, supplied: dict) -> dict:
    env = dict(current or {})
    env["MCP_TRANSPORT"] = "streamable_http"
    env.setdefault("TABLE_NAME", "io-travel-flights")
    for k in KEYS:
        if supplied.get(k):
            env[k] = supplied[k]
    return {k: v for k, v in env.items() if v and k not in DROPPED}


def guard(env: dict) -> str:
    if env.get("COGNITO_USER_POOL_ID") and not (env.get("COGNITO_ALLOWED_SUBS") or "").strip(" ,"):
        return "refusing to deploy: COGNITO_USER_POOL_ID is set but COGNITO_ALLOWED_SUBS is empty (would lock out the Cognito path)"
    return ""


def main() -> int:
    raw = os.environ.get("CURRENT_ENV", "") or "{}"
    try:
        current = json.loads(raw)
        if current is None:
            current = {}
    except ValueError:
        current = {}
    env = resolve(current, os.environ)
    msg = guard(env)
    if msg:
        print(msg, file=sys.stderr)
        return 3
    print(json.dumps({"Variables": env}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
