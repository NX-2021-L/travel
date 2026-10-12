"""DVP-TSK-903: Enceladus pool + io-graph pool accepted, each with own client and allow-list."""
import io
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

import test_e2e_lambda_handler as base  # reuses moto env + rpc helpers
from test_e2e_lambda_handler import env, lf, payload, rpc  # noqa: F401

IOG_KID, IOG_ISS, IOG_CLIENT = "iogkid", lf.IOGRAPH_ISSUER, lf.IOGRAPH_CLIENT_ID


def _tok(priv, kid, iss, sub, client, use="access"):
    now = int(time.time())
    c = {"sub": sub, "iss": iss, "token_use": use, "iat": now, "exp": now + 600}
    c["client_id" if use == "access" else "aud"] = client
    return jwt.encode(c, priv, algorithm="RS256", headers={"kid": kid})


@pytest.fixture
def dual(env, monkeypatch):  # noqa: F811
    enc_priv, _ = env
    iog = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(iog.public_key()))
    jwk.update({"kid": IOG_KID, "alg": "RS256", "use": "sig"})
    iog_jwks = json.dumps({"keys": [jwk]}).encode()
    base.JWKS[IOG_ISS] = json.loads(iog_jwks)
    monkeypatch.setenv("IOGRAPH_ALLOWED_SUBS", "io-sub")
    return enc_priv, iog, other


def _get(bearer):
    return rpc(bearer, "get_flight", {"flight_id": "abc"})


def test_iograph_listed_sub_reads(dual):
    _, iog, _ = dual
    assert payload(_get(_tok(iog, IOG_KID, IOG_ISS, "io-sub", IOG_CLIENT)))["result"]["flight_id"] == "abc"


def test_iograph_id_token_reads(dual):
    _, iog, _ = dual
    assert payload(_get(_tok(iog, IOG_KID, IOG_ISS, "io-sub", IOG_CLIENT, use="id")))["success"]


def test_iograph_unlisted_sub_403(dual):
    _, iog, _ = dual
    assert _get(_tok(iog, IOG_KID, IOG_ISS, "stranger", IOG_CLIENT))["statusCode"] == 403


def test_iograph_wrong_client_401(dual):
    _, iog, _ = dual
    assert _get(_tok(iog, IOG_KID, IOG_ISS, "io-sub", base.CLIENT))["statusCode"] == 401


def test_allow_lists_are_per_pool(dual):
    enc, iog, _ = dual
    assert _get(_tok(iog, IOG_KID, IOG_ISS, "owner-sub", IOG_CLIENT))["statusCode"] == 403
    assert _get(_tok(enc, base.KID, base.ISS, "io-sub", base.CLIENT))["statusCode"] == 403


def test_foreign_issuer_401(dual):
    _, _, other = dual
    iss = "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_EVIL"
    assert _get(_tok(other, IOG_KID, iss, "io-sub", IOG_CLIENT))["statusCode"] == 401


def test_iograph_issuer_wrong_signing_key_401(dual):
    _, _, other = dual
    assert _get(_tok(other, IOG_KID, IOG_ISS, "io-sub", IOG_CLIENT))["statusCode"] == 401


def test_enceladus_pool_still_works(dual):
    enc, _, _ = dual
    assert payload(_get(_tok(enc, base.KID, base.ISS, "owner-sub", base.CLIENT)))["success"]








def _event(method, origin, bearer=None):
    h = {"origin": origin}
    if bearer:
        h["authorization"] = f"Bearer {bearer}"
    return {"requestContext": {"http": {"method": method, "path": "/mcp"}}, "rawPath": "/mcp",
            "headers": h, "body": "", "isBase64Encoded": False}


def test_cors_preflight_allows_think_origin(dual):
    r = lf.lambda_handler(_event("OPTIONS", "https://think.thepup.io"), None)
    assert r["statusCode"] == 204
    assert r["headers"]["Access-Control-Allow-Origin"] == "https://think.thepup.io"


def test_cors_other_origin_gets_no_acao(dual):
    r = lf.lambda_handler(_event("OPTIONS", "https://evil.example"), None)
    assert "Access-Control-Allow-Origin" not in r["headers"]


def test_cors_headers_on_401(dual):
    r = lf.lambda_handler(_event("POST", "https://think.thepup.io"), None)
    assert r["statusCode"] == 401
    assert r["headers"]["Access-Control-Allow-Origin"] == "https://think.thepup.io"
