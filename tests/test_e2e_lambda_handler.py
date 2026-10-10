"""E2-R12: UNPATCHED end-to-end tests through the real lambda_handler.

Only the JWKS network fetch and the data store (moto) are faked.  Principal
plumbing, auth, policy and the MCP session manager are all real.
"""
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ["TABLE_NAME"] = "io-travel-flights"
D = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import lambda_function as lf  # noqa: E402

POOL, CLIENT, REGION, KID = "us-east-1_TESTPOOL", "client123", "us-east-1", "kid1"
ISS = f"https://cognito-idp.{REGION}.amazonaws.com/{POOL}"
KEY = "machine-key-0123456789"


@pytest.fixture
def env(monkeypatch):
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(RSAAlgorithm.to_jwk(priv.public_key()))
    jwk.update({"kid": KID, "alg": "RS256", "use": "sig"})
    jwks = json.dumps({"keys": [jwk]}).encode()

    class Resp(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(lf.urllib.request, "urlopen", lambda *a, **k: Resp(jwks))
    monkeypatch.setattr(lf, "COGNITO_USER_POOL_ID", POOL)
    monkeypatch.setattr(lf, "COGNITO_CLIENT_ID", CLIENT)
    monkeypatch.setattr(lf, "COGNITO_REGION", REGION)
    monkeypatch.setattr(lf, "MCP_API_KEY", KEY)
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", ["owner-sub"])
    monkeypatch.setattr(lf, "TRAVEL_WRITE_GROUPS", [])
    monkeypatch.setattr(lf, "_cognito_jwks_cache", {})
    monkeypatch.setattr(lf, "_cognito_jwks_fetched_at", 0.0)
    with mock_aws():
        lf._dynamo_resource = None
        t = boto3.resource("dynamodb", region_name="us-west-2").create_table(
            TableName="io-travel-flights",
            KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        t.put_item(Item={"flight_id": "abc", "origin": "SEA", "dest": "SFO", "status": "active"})
        yield priv, t
        lf._dynamo_resource = None


def token(priv, sub):
    now = int(time.time())
    return jwt.encode({"sub": sub, "iss": ISS, "token_use": "access", "client_id": CLIENT,
                       "iat": now, "exp": now + 600}, priv, algorithm="RS256", headers={"kid": KID})


def rpc(bearer, name, arguments):
    ev = {
        "requestContext": {"http": {"method": "POST", "path": "/mcp"}},
        "headers": {"authorization": f"Bearer {bearer}", "content-type": "application/json",
                    "accept": "application/json, text/event-stream"},
        "body": json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                            "params": {"name": name, "arguments": arguments}}),
        "isBase64Encoded": False,
    }
    return lf.lambda_handler(ev, None)


def payload(resp):
    assert resp["statusCode"] == 200, resp["body"]
    result = json.loads(resp["body"])["result"]
    return json.loads(result["content"][0]["text"])


@pytest.mark.parametrize("who", ["machine", "owner"])
def test_read_succeeds(env, who):
    priv, table = env
    bearer = KEY if who == "machine" else token(priv, "owner-sub")
    out = payload(rpc(bearer, "get_flight", {"flight_id": "abc"}))
    assert out["success"] is True and out["result"]["flight_id"] == "abc"


@pytest.mark.skipif(not hasattr(lf, "_split_control"), reason="dry_run lands with DVP-TSK-900")
@pytest.mark.parametrize("who", ["machine", "owner"])
def test_dry_run_succeeds(env, who):
    priv, table = env
    bearer = KEY if who == "machine" else token(priv, "owner-sub")
    out = payload(rpc(bearer, "cancel_flight", {"flight_id": "abc", "dry_run": True}))
    assert out["success"] is True and out["result"]["resolved_call"]["v"] == 1
    assert table.get_item(Key={"flight_id": "abc"})["Item"]["status"] == "active"


def test_owner_write_succeeds(env):
    priv, table = env
    out = payload(rpc(token(priv, "owner-sub"), "cancel_flight", {"flight_id": "abc"}))
    assert out["success"] is True
    assert table.get_item(Key={"flight_id": "abc"})["Item"]["status"] == "cancelled"


def test_non_listed_sub_denied(env):
    priv, table = env
    resp = rpc(token(priv, "coach-sub"), "get_flight", {"flight_id": "abc"})
    assert resp["statusCode"] == 403
    assert "result" not in json.loads(resp["body"])


def test_unset_allow_list_denies_owner(env, monkeypatch):
    priv, _ = env
    monkeypatch.setattr(lf, "COGNITO_ALLOWED_SUBS", [])
    assert rpc(token(priv, "owner-sub"), "get_flight", {"flight_id": "abc"})["statusCode"] == 403


def test_principal_holder_reset_after_request(env):
    rpc(KEY, "get_flight", {"flight_id": "abc"})
    assert lf._PRINCIPAL.get() is None


# --- deploy env resolution / guard ---
sys.path.insert(0, str(D))
import resolve_env  # noqa: E402


def test_resolve_env_preserves_allow_list():
    cur = {"COGNITO_USER_POOL_ID": "p", "COGNITO_ALLOWED_SUBS": "a,b", "EXTRA": "keep"}
    env = resolve_env.resolve(cur, {"MCP_API_KEY": "k"})
    assert env["COGNITO_ALLOWED_SUBS"] == "a,b" and env["EXTRA"] == "keep" and env["MCP_API_KEY"] == "k"
    assert resolve_env.guard(env) == ""
    assert resolve_env.resolve(cur, {"COGNITO_ALLOWED_SUBS": "z"})["COGNITO_ALLOWED_SUBS"] == "z"


def test_resolve_env_guard_refuses_empty_allow_list():
    env = resolve_env.resolve({"COGNITO_USER_POOL_ID": "p"}, {})
    assert "COGNITO_ALLOWED_SUBS" in resolve_env.guard(env)
    r = subprocess.run([sys.executable, str(D / "resolve_env.py")], capture_output=True,
                       env={"CURRENT_ENV": json.dumps({"COGNITO_USER_POOL_ID": "p"}), "PATH": os.environ["PATH"]})
    assert r.returncode == 3 and r.stdout == b""
    assert resolve_env.guard(resolve_env.resolve({}, {})) == ""
