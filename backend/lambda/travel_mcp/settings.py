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


def missing_required(environ=None) -> list:
    """Keys of required contract settings that are absent or blank in `environ`."""
    env = _env(environ)
    return [k for k, spec in contract.SETTINGS.items() if spec["required"] and _blank(env.get(k))]


def missing_runtime(environ=None) -> list:
    env = _env(environ)
    return [k for k in RUNTIME_SETTINGS if _blank(env.get(k) or env.get("AWS_DEFAULT_REGION"))]


def failure_message(missing: list) -> str:
    return "refusing to start: required settings absent: " + ", ".join(sorted(missing))


def load(environ=None) -> dict:
    """Every contract setting (optional ones as "") plus the runtime region; raises if any required is absent."""
    env = _env(environ)
    missing = missing_required(env) + missing_runtime(env)
    if missing:
        raise SettingsError(failure_message(missing))
    out = {k: (env.get(k) or "").strip() for k in contract.SETTINGS}
    out["AWS_REGION"] = (env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")).strip()
    return out
