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
for _k, _v in _TEST_DEPLOYMENT.items():
    os.environ.setdefault(_k, _v)
