"""Human-door verification for the read-only /records routes (FLY-TSK-083).

Accepts ONE kind of credential: an @io-kit/auth human-door token (ES256 JWT, JOSE typ door+jwt) whose issuer is in the
configured issuer list, whose audience is travel's configured audience, whose principal_type is human and which has not
expired. API keys, connector bearers (Cognito RS256) and a missing token are all refused.

verify_door_token mirrors io-kit packages/records/python/src/io_kit_records/door.py (branch agent/dvp-tsk-m06-0c) rule for
rule. It should be replaced by `from io_kit_records.door import verify_door_token` once io-kit-records is published.

Settings (read from the environment, no defaults; any absent one refuses every request, fail closed):
  RECORDS_DOOR_ISSUERS       comma-separated accepted issuers
  RECORDS_DOOR_AUDIENCE      the one audience travel accepts
  RECORDS_DOOR_PUBLIC_KEYS   JSON object {issuer: ES256 public key as JWK object or PEM text}
Messages never carry a value.
"""
import json
import os
import re
from typing import Any

TOKEN_TYP = "door+jwt"
ALGORITHM = "ES256"
ISSUERS_KEY, AUDIENCE_KEY, KEYS_KEY = "RECORDS_DOOR_ISSUERS", "RECORDS_DOOR_AUDIENCE", "RECORDS_DOOR_PUBLIC_KEYS"


class DoorTokenError(Exception):
    """Every refused token or unusable configuration; ``reason`` is for the server log, never the response."""

    def __init__(self, reason: str, message: str = None) -> None:
        super().__init__(message or reason)
        self.reason = reason


def _load_key(public_key: Any) -> Any:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.serialization import load_pem_public_key
    from jwt import PyJWK

    try:
        if isinstance(public_key, (str, bytes)):
            text = public_key.decode() if isinstance(public_key, bytes) else public_key
            key = PyJWK.from_dict(json.loads(text)).key if text.lstrip().startswith("{") else load_pem_public_key(text.encode())
        elif isinstance(public_key, dict):
            key = PyJWK.from_dict(public_key).key
        else:
            key = public_key
    except Exception as e:  # noqa: BLE001 - any unreadable key is a config error
        raise DoorTokenError("door_config", "unreadable public key") from e
    if isinstance(key, ec.EllipticCurvePrivateKey):
        key = key.public_key()
    if not isinstance(key, ec.EllipticCurvePublicKey) or key.curve.name != "secp256r1":
        raise DoorTokenError("door_config", "the key must be an ES256 (P-256) public key")
    return key


def verify_door_token(token: str, issuer: str, audience: str, public_key: Any) -> dict:
    """Return the claims, or raise DoorTokenError (wrong issuer, audience, key, expiry, typ or principal type)."""
    import jwt

    if not issuer or not audience or not public_key:
        raise DoorTokenError("door_config", "issuer, audience and public_key required")
    key = _load_key(public_key)
    try:
        header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as e:
        raise DoorTokenError("door_token_invalid") from e
    if header.get("alg") != ALGORITHM:
        raise DoorTokenError("door_token_invalid", "wrong alg")
    if header.get("typ") != TOKEN_TYP:
        raise DoorTokenError("door_token_bad_typ", "wrong typ")
    try:
        claims = jwt.decode(token, key, algorithms=[ALGORITHM], issuer=issuer, audience=audience, leeway=0,
                            options={"require": ["exp", "iat", "sub", "iss", "aud"]})
    except jwt.ExpiredSignatureError as e:
        raise DoorTokenError("door_token_expired") from e
    except jwt.InvalidSignatureError as e:
        raise DoorTokenError("door_token_bad_signature") from e
    except jwt.InvalidAudienceError as e:
        raise DoorTokenError("door_token_bad_aud") from e
    except jwt.InvalidIssuerError as e:
        raise DoorTokenError("door_token_bad_iss") from e
    except jwt.InvalidTokenError as e:
        raise DoorTokenError("door_token_invalid") from e
    aud = claims.get("aud")
    if isinstance(aud, list):
        if len(aud) != 1:
            raise DoorTokenError("door_token_bad_aud")
        claims["aud"] = aud[0]
    if claims.get("principal_type") != "human":
        raise DoorTokenError("door_token_not_human", "principal type is not human")
    w = claims.get("window_opened_at")
    if not isinstance(w, (int, float)) or isinstance(w, bool):
        raise DoorTokenError("door_token_bad_window_opened_at")
    return claims


def _config(environ) -> tuple:
    env = os.environ if environ is None else environ
    issuers = [i.strip() for i in str(env.get(ISSUERS_KEY) or "").split(",") if i.strip()]
    audience = str(env.get(AUDIENCE_KEY) or "").strip()
    raw = str(env.get(KEYS_KEY) or "").strip()
    if not issuers or not audience or not raw:
        raise DoorTokenError("door_config", "records door settings absent")
    try:
        keys = json.loads(raw)
    except ValueError as e:
        raise DoorTokenError("door_config", "records door keys are not JSON") from e
    if not isinstance(keys, dict):
        raise DoorTokenError("door_config", "records door keys must be an object")
    return issuers, audience, keys


_BEARER = re.compile(r"^bearer[ \t]+(\S+)[ \t]*$", re.I)


def authenticate(headers: dict, environ=None) -> dict:
    """The claims of a valid human-door token in the Authorization header; DoorTokenError otherwise."""
    h = {str(k).lower(): str(v) for k, v in (headers or {}).items() if v is not None}
    m = _BEARER.match(h.get("authorization", ""))
    if not m:
        raise DoorTokenError("door_token_missing")
    issuers, audience, keys = _config(environ)
    import base64
    try:
        seg = m.group(1).split(".")[1]
        iss = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4))).get("iss")
    except Exception as e:  # noqa: BLE001 - an API key or a malformed token has no readable payload
        raise DoorTokenError("door_token_invalid") from e
    if not isinstance(iss, str) or iss not in issuers:
        raise DoorTokenError("door_token_bad_iss")
    return verify_door_token(m.group(1), iss, audience, keys.get(iss))
