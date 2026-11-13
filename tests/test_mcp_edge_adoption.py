"""FLY-TSK-022: io-travel runs on the io-kit MCP edge (Pattern B, public PKCE client). No secret anywhere."""
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

import test_e2e_lambda_handler as base
from test_e2e_lambda_handler import env, lf, payload, rpc  # noqa: F401

IOG_CLIENT = "6eh4u69l1mqk1m4hfjspeibla8"
IOG_ISS = "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_y6bHKpNZN"
KID = "edgekid"
SECRET_MARKERS = ("client_secret", "COGNITO_CLIENT_SECRET", "registration_access_token")


@pytest.fixture
def edge(env, monkeypatch):  # noqa: F811
    iog = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(iog.public_key()))
    jwk.update({"kid": KID, "alg": "RS256", "use": "sig"})
    base.JWKS[IOG_ISS] = {"keys": [jwk]}
    monkeypatch.setenv("IOGRAPH_ALLOWED_SUBS", "io-owner")
    return iog


def _tok(priv, sub="io-owner", client=IOG_CLIENT):
    now = int(time.time())
    return jwt.encode({"sub": sub, "iss": IOG_ISS, "token_use": "access", "client_id": client,
                       "iat": now, "exp": now + 600}, priv, algorithm="RS256", headers={"kid": KID})


def _call(method, path, bearer=None, body=""):
    h = {"content-type": "application/json", "accept": "application/json, text/event-stream"}
    if bearer:
        h["authorization"] = f"Bearer {bearer}"
    return lf.lambda_handler({"requestContext": {"http": {"method": method, "path": path}}, "rawPath": path,
                              "headers": h, "body": body, "isBase64Encoded": False}, None)


def _hdr(resp, name):
    return {k.lower(): v for k, v in resp["headers"].items()}.get(name)


@pytest.mark.parametrize("method", ["GET", "POST"])
# 0.2: /register, /authorize and the authorization-server metadata are the sign-in service's (connect.thepup.io); here they hit the gate.
@pytest.mark.parametrize("path", ["/oauth/register", "/oauth/token", "/callback", "/register", "/authorize",
                                  "/.well-known/oauth-authorization-server", "/mcp", "/", "/caps.json"])
def test_former_open_paths_deny_anonymous(edge, method, path):
    r = _call(method, path, body="{}" if method == "POST" else "")
    assert r["statusCode"] == 401
    assert _hdr(r, "www-authenticate").startswith("Bearer ")
    assert _hdr(r, "x-io-kit-mcp")
    assert not any(m in r["body"] for m in SECRET_MARKERS)


@pytest.mark.parametrize("path", ["/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"])
def test_discovery_names_the_shared_sign_in_service(edge, path):
    r = _call("GET", path)
    prm = json.loads(r["body"])
    assert prm["resource"] == "https://travel.thepup.io/mcp"
    assert prm["authorization_servers"] == ["https://connect.thepup.io"]
    assert _hdr(r, "x-io-kit-mcp")


@pytest.mark.parametrize("path", ["/mcp", "/"])  # /mcp is canonical; / stays until io reconnects
def test_valid_iograph_token_reaches_tools(edge, path):
    out = payload(rpc(_tok(edge), "get_flight", {"flight_id": "abc"}, path=path))
    assert out["success"] is True and out["result"]["flight_id"] == "abc"


def test_bad_tokens_and_unlisted_sub_denied(edge):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    assert rpc(_tok(other), "get_flight", {"flight_id": "abc"})["statusCode"] == 401        # wrong signing key
    assert rpc(_tok(edge, client="someone-else"), "get_flight", {"flight_id": "abc"})["statusCode"] == 401  # client binding
    assert rpc(_tok(edge, sub="coach9"), "get_flight", {"flight_id": "abc"})["statusCode"] == 403  # allow-list
    assert rpc("not-a-jwt", "get_flight", {"flight_id": "abc"})["statusCode"] == 401        # static keys no longer accepted


def test_no_secret_left_in_source():
    src = (base.D / "lambda_function.py").read_text() + (base.D / "deploy.sh").read_text()
    assert "COGNITO_CLIENT_SECRET" not in src and "client_secret_post" not in src
