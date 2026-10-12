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
# /authorize is deliberately public again since FLY-TSK-022: the kit's secretless 302 to Cognito (F4b); see
# test_authorize_redirect_* below. It never proxies a token and never carries a secret.
@pytest.mark.parametrize("path", ["/oauth/register", "/oauth/token", "/callback", "/mcp", "/", "/caps.json"])
def test_former_open_paths_deny_anonymous(edge, method, path):
    r = _call(method, path, body="{}" if method == "POST" else "")
    assert r["statusCode"] in (401, 404)
    if r["statusCode"] == 401:
        assert _hdr(r, "www-authenticate").startswith("Bearer ")
    assert not any(m in r["body"] for m in SECRET_MARKERS)


def test_discovery_matches_iograph_as(edge):
    prm = json.loads(_call("GET", "/.well-known/oauth-protected-resource")["body"])
    assert prm["resource"] == "https://travel.thepup.io" and prm["authorization_servers"] == ["https://travel.thepup.io"]
    asm = json.loads(_call("GET", "/.well-known/oauth-authorization-server")["body"])
    assert asm["authorization_endpoint"] == "https://travel.thepup.io/authorize"  # F4b authorize redirect
    assert asm["token_endpoint"] == "https://io-graph-iograph.auth.us-west-2.amazoncognito.com/oauth2/token"
    assert asm["revocation_endpoint"] == "https://io-graph-iograph.auth.us-west-2.amazoncognito.com/oauth2/revoke"
    assert asm["jwks_uri"] == IOG_ISS + "/.well-known/jwks.json"
    assert asm["token_endpoint_auth_methods_supported"] == ["none"]
    assert asm["registration_endpoint"] == "https://travel.thepup.io/register"


def test_register_facade_hands_out_public_client_no_secret(edge):
    r = _call("POST", "/register", body=json.dumps({"client_name": "claude", "redirect_uris": ["https://claude.ai/api/mcp/auth_callback"]}))
    doc = json.loads(r["body"])
    assert r["statusCode"] == 201 and doc["client_id"] == IOG_CLIENT and doc["token_endpoint_auth_method"] == "none"
    assert not any(m in r["body"] for m in SECRET_MARKERS)


def test_valid_iograph_token_reaches_tools(edge):
    out = payload(rpc(_tok(edge), "get_flight", {"flight_id": "abc"}, path="/"))
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


def test_authorize_redirect_drops_resource(env):
    """F4b: Cognito managed login v2 refuses codes minted from an authorize that carried resource."""
    ev = {"version": "2.0", "rawPath": "/authorize", "rawQueryString": "response_type=code&client_id=6eh4u69l1mqk1m4hfjspeibla8&state=s&resource=https%3A%2F%2Ftravel.thepup.io",
          "headers": {}, "requestContext": {"http": {"method": "GET"}, "stage": "$default"}}
    res = lf.lambda_handler(ev, None)
    assert res["statusCode"] == 302
    assert res["headers"]["location"] == "https://io-graph-iograph.auth.us-west-2.amazoncognito.com/oauth2/authorize?response_type=code&client_id=6eh4u69l1mqk1m4hfjspeibla8&state=s"


def test_authorize_redirect_is_secretless_and_get_only(edge):
    r = _call("GET", "/authorize")
    assert r["statusCode"] == 302 and not any(m in (r.get("body") or "") for m in SECRET_MARKERS)
    assert _hdr(r, "location").startswith("https://io-graph-iograph.auth.us-west-2.amazoncognito.com/oauth2/authorize")
    assert _call("POST", "/authorize", body="{}")["statusCode"] == 405
