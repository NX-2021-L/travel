"""Test environment for a travel deployment (FLY-TSK-028-0B).

lambda_function REFUSES TO START without every required setting (contract.SETTINGS), so the test
process plays the part of a deployment and supplies them, before any test module imports it. These
are TEST values (this file is a fixture, excluded from tests/test_no_instance_values.py). Tests that
need an absent setting build their own environment and never rely on these.
"""
import os

_TEST_DEPLOYMENT = {
    "TABLE_NAME": "io-travel-flights",
    "RECORDS_TABLE_NAME": "io-travel-records",
    "SURFACE_NAME": "io-travel",
    "FINANCE_READ_FUNCTION": "finance-mcp",
    "COGNITO_USER_POOL_ID": "us-east-1_TESTPOOL",
    "COGNITO_CLIENT_ID": "client123",
    "COGNITO_REGION": "us-east-1",
    "COGNITO_ALLOWED_SUBS": "owner-sub",
    "CORS_ORIGINS": "https://think.thepup.io",
    "DOOR_MODE": "public",
    "AWS_REGION": "us-west-2",
}
# io-kit ships no fleet registry (ENC-ISS-919): the public edge's connector comes from IO_KIT_FLEET_JSON, which io's
# settings file supplies. Tests use that same registry so they exercise what io's deployment deploys.
import re  # noqa: E402
from pathlib import Path  # noqa: E402

_m = re.search(r"^IO_KIT_FLEET_JSON=(.+)$", (Path(__file__).resolve().parents[1] / "deploy" / "settings.env").read_text(), re.M)
if _m:
    _TEST_DEPLOYMENT["IO_KIT_FLEET_JSON"] = _m.group(1).strip()
for _k, _v in _TEST_DEPLOYMENT.items():
    os.environ.setdefault(_k, _v)
