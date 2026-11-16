"""Fail-closed settings for one travel deployment (FLY-TSK-028-0B, BRD DOC-14994CDDA217 7.2 lane 1A, A-2).

The artifact names no deployment: every value that identifies one (tables, surface, sign-in pool,
finance function, allow-lists, public URL, origins) arrives in the environment, keyed by
contract.SETTINGS. This module is stdlib-only. lambda_function calls load() at import, so a
deployment missing a required setting REFUSES TO START: the cold start raises SettingsError and the
function serves nothing, rather than serving a degraded tool set or another deployment's values.
resolve_env.py and scripts/release_ops.py call missing_required() so the deploy stops before apply.
Messages name KEYS only, never values.
"""
import os

import contract  # a package without contract.py must fail to start, not degrade

# Provided by the Lambda runtime itself, not by the deployment; required all the same.
RUNTIME_SETTINGS = ("AWS_REGION",)


class SettingsError(RuntimeError):
    """A required setting is absent or blank; the function must not start."""


def _blank(value) -> bool:
    return not str(value or "").strip(" ,\t\n")


def _env(environ):
    return os.environ if environ is None else environ


def _required_here(spec, mode) -> bool:
    """True/False apply to every door; a string names the one DOOR_MODE that requires the setting."""
    req = spec["required"]
    return req == mode if isinstance(req, str) else bool(req)


def missing_required(environ=None) -> list:
    """Keys of contract settings required under this environment's DOOR_MODE that are absent or blank.

    A door-named setting is required on that door only. An absent or unknown DOOR_MODE is read as public,
    the strictest reading: door.py refuses to start on it regardless (lane 1A, A-2; FLY-TSK-043).
    """
    env = _env(environ)
    mode = (env.get("DOOR_MODE") or "").strip()
    if mode not in ("public", "internal"):
        mode = "public"
    return [k for k, spec in contract.SETTINGS.items() if _required_here(spec, mode) and _blank(env.get(k))]


def missing_runtime(environ=None) -> list:
    env = _env(environ)
    return [k for k in RUNTIME_SETTINGS if _blank(env.get(k) or env.get("AWS_DEFAULT_REGION"))]


def region(environ=None) -> str:
    """The deployment's AWS region, from the runtime. No default exists: absent raises SettingsError (A-14)."""
    env = _env(environ)
    value = (env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or "").strip()
    if not value:
        raise SettingsError(failure_message(list(RUNTIME_SETTINGS)))
    return value


def failure_message(missing: list) -> str:
    return "refusing to start: required settings absent: " + ", ".join(sorted(missing))


def load(environ=None) -> dict:
    """Every contract setting (optional ones as "") plus the runtime region; raises if any required is absent."""
    env = _env(environ)
    missing = missing_required(env) + missing_runtime(env)
    if missing:
        raise SettingsError(failure_message(missing))
    out = {k: (env.get(k) or "").strip() for k in contract.SETTINGS}
    out["AWS_REGION"] = region(env)
    return out
