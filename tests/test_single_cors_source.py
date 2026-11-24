"""FLY-ISS-005: the handler is the ONLY source of CORS headers.

A CORS config on the Function URL added a second access-control-allow-origin to every non-preflight response
(the handler adds the allow-listed one), and browsers reject a doubled header: think's records page showed
"Failed to fetch" for Travel. With the Function URL's CORS gone, the preflight reaches the handler too, so the
handler must answer it for the records routes and for /mcp, exactly once, and only for allow-listed origins."""
import re
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import records_fixture as fx  # noqa: E402,F401  (puts travel_mcp on sys.path)
try:
    import io_kit_mcp_edge  # noqa: F401
except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
    _stub = types.ModuleType("io_kit_mcp_edge")
    for _n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
        setattr(_stub, _n, type(_n, (Exception,), {}))
    _jw = types.ModuleType("io_kit_mcp_edge.jwks")
    _jw.fetch_https = lambda *a, **k: {}
    sys.modules["io_kit_mcp_edge"], sys.modules["io_kit_mcp_edge.jwks"] = _stub, _jw

import lambda_function as lf  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
THINK = "https://think.thepup.io"


@pytest.fixture(autouse=True)
def world(monkeypatch):
    for k, v in fx.env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("CORS_ORIGINS", THINK)
    fx.install(monkeypatch)
    if lf.DOOR_MODE != "public":
        pytest.skip("public door only")


def _ev(method, path, origin, token=None):
    h = {"origin": origin}
    if token:
        h["authorization"] = f"Bearer {token}"
    return {"requestContext": {"http": {"method": method, "path": path}}, "rawPath": path, "headers": h}


def _acao(resp):
    return [v for k, v in (resp.get("headers") or {}).items() if k.lower() == "access-control-allow-origin"]


@pytest.mark.parametrize("path", ["/records/recordsets", "/records/recordsets/trips/rows"])
def test_records_preflight_is_answered_by_the_handler(path):
    r = lf.lambda_handler(_ev("OPTIONS", path, THINK), None)
    assert r["statusCode"] == 204
    assert _acao(r) == [THINK]
    h = {k.lower(): v for k, v in r["headers"].items()}
    assert "authorization" in h["access-control-allow-headers"].lower()
    assert "GET" in h["access-control-allow-methods"]


@pytest.mark.parametrize("token", [None, "good"])
def test_records_get_carries_one_acao(token):
    r = lf.lambda_handler(_ev("GET", "/records/recordsets", THINK, fx.token() if token else None), None)
    assert r["statusCode"] == (200 if token else 401)
    assert _acao(r) == [THINK]


def test_other_origin_gets_no_cors_on_records():
    for method in ("OPTIONS", "GET"):
        r = lf.lambda_handler(_ev(method, "/records/recordsets", "https://evil.example"), None)
        assert _acao(r) == []


def test_function_url_has_no_cors_in_the_template():
    from test_travel_deployment_template import R
    assert "Cors" not in R["FunctionUrl"]["Properties"]


def test_deploy_script_creates_no_url_cors_and_clears_it():
    sh = (ROOT / "backend/lambda/travel_mcp/deploy.sh").read_text()
    assert '"AllowOrigins"' not in sh
    assert re.search(r"update-function-url-config[\s\\]+--function-name \"\$\{FUNCTION_NAME\}\"[\s\\]+--cors '\{\}'", sh)


def test_verify_checks_one_acao_header():
    wf = (ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text()
    assert "access-control-allow-origin headers (want 1)" in wf
