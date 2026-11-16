"""Offline checks of infra/cloudformation/travel-deployment.yaml (FLY-TSK-028-0C, criterion A-3)."""
import json
import pathlib
import re

import yaml

TEMPLATE = pathlib.Path(__file__).resolve().parent.parent / "infra" / "cloudformation" / "travel-deployment.yaml"


class _Loader(yaml.SafeLoader):
    pass


def _intrinsic(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        val = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        val = loader.construct_sequence(node, deep=True)
    else:
        val = loader.construct_mapping(node, deep=True)
    name = "Ref" if suffix == "Ref" else "Fn::" + suffix
    return {name: val}


_Loader.add_multi_constructor("!", _intrinsic)


def load():
    return yaml.load(TEMPLATE.read_text(), Loader=_Loader)


T = load()
R = T["Resources"]
TABLES = {k: v for k, v in R.items() if v["Type"] == "AWS::DynamoDB::Table"}


def test_two_tables():
    assert set(TABLES) == {"FlightsTable", "RecordsTable"}


def test_tables_have_recovery_and_deletion_protection_and_retain():
    for name, res in TABLES.items():
        p = res["Properties"]
        assert p["PointInTimeRecoverySpecification"] == {"PointInTimeRecoveryEnabled": True}, name
        assert p["DeletionProtectionEnabled"] is True, name
        assert res["DeletionPolicy"] == "Retain", name
        assert res["UpdateReplacePolicy"] == "Retain", name
        assert p["BillingMode"] == "PAY_PER_REQUEST", name


def _gsis(props):
    return {
        g["IndexName"]: ([k["AttributeName"] for k in g["KeySchema"]], g["Projection"]["ProjectionType"])
        for g in props["GlobalSecondaryIndexes"]
    }


def test_flights_table_matches_live_shape():
    p = R["FlightsTable"]["Properties"]
    assert p["KeySchema"] == [{"AttributeName": "flight_id", "KeyType": "HASH"}]
    assert _gsis(p) == {
        "date-index": (["date_yyyy_mm", "date_iso"], "ALL"),
        "route-index": (["origin", "dest"], "ALL"),
        "trip-city-index": (["trip_city", "date_iso"], "ALL"),
    }
    assert all(a["AttributeType"] == "S" for a in p["AttributeDefinitions"])


def test_records_table_single_table_with_area_index():
    p = R["RecordsTable"]["Properties"]
    assert [k["AttributeName"] for k in p["KeySchema"]] == ["pk", "sk"]
    assert _gsis(p) == {"area-index": (["area_id", "date_iso"], "ALL")}


def test_function_url_and_permission_only_for_public_edge():
    assert T["Conditions"]["IsPublicEdge"] == {"Fn::Equals": [{"Ref": "PublicEdge"}, "true"]}
    assert T["Parameters"]["PublicEdge"]["Default"] == "false"
    guarded = {k for k, v in R.items() if v.get("Condition") == "IsPublicEdge"}
    assert guarded == {"FunctionUrl", "FunctionUrlPublicInvoke"}
    assert R["FunctionUrl"]["Type"] == "AWS::Lambda::Url"
    assert R["FunctionUrlPublicInvoke"]["Type"] == "AWS::Lambda::Permission"
    # no other resource of either type exists unguarded
    for k, v in R.items():
        if v["Type"] in ("AWS::Lambda::Url", "AWS::Lambda::Permission"):
            assert v.get("Condition") == "IsPublicEdge", k
    assert T["Outputs"]["FunctionUrl"]["Condition"] == "IsPublicEdge"


def test_no_io_instance_value_hardcoded():
    text = TEMPLATE.read_text()
    for needle in ("io-travel", "356364570033", "us-west-2", "E2DDNP2MQ5IZCS", "product-lead"):
        assert needle not in text, needle
    for name in ("FlightsTableName", "RecordsTableName", "FunctionName", "RoleName"):
        assert "Default" not in T["Parameters"][name]
    for k in ("FlightsTable", "RecordsTable"):
        assert R[k]["Properties"]["TableName"] == {"Ref": k + "Name"}
    assert R["McpFunction"]["Properties"]["FunctionName"] == {"Ref": "FunctionName"}
    assert R["ExecutionRole"]["Properties"]["RoleName"] == {"Ref": "RoleName"}


def test_role_is_scoped_to_the_two_tables():
    stmts = R["ExecutionRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    dumped = json.dumps(stmts)
    assert '"*"' not in dumped
    assert "dynamodb:*" not in dumped
    assert "FlightsTable" in dumped and "RecordsTable" in dumped


def test_no_generated_or_timestamped_properties():
    """Idempotency: nothing in the template varies between applications of the same inputs."""
    text = TEMPLATE.read_text()
    assert not re.search(r"Fn::(Sub|Join)[^\n]*(timestamp|date|uuid|random)", text, re.I)
    assert "AWS::StackId" not in text and "AWS::StackName" not in text
    for k, v in R.items():
        props = v.get("Properties", {})
        name_key = {"AWS::DynamoDB::Table": "TableName", "AWS::Lambda::Function": "FunctionName",
                    "AWS::IAM::Role": "RoleName"}.get(v["Type"])
        if name_key:
            assert name_key in props, f"{k} must have an explicit, parameterised name"



# ---- FLY-TSK-028-0D, A-4 clause 3: no template gives an internal function a public grant ----
def _created(resource, public_edge):
    """Would CloudFormation create `resource` for this PublicEdge value? Evaluates the condition."""
    cond = R[resource].get("Condition")
    assert cond, resource + " must be conditional"
    assert T["Conditions"][cond] == {"Fn::Equals": [{"Ref": "PublicEdge"}, "true"]}
    return public_edge == "true"


def test_internal_deployment_creates_no_function_url_and_no_public_invoke():
    assert T["Parameters"]["PublicEdge"]["Default"] == "false"
    for res in ("FunctionUrl", "FunctionUrlPublicInvoke"):
        assert _created(res, "false") is False  # PublicEdge=false: neither resource is created
        assert _created(res, "true") is True


def test_every_public_grant_in_the_template_is_behind_the_public_edge_condition():
    for name, res in R.items():
        p = res.get("Properties", {})
        public = res["Type"] == "AWS::Lambda::Url" or (
            res["Type"] == "AWS::Lambda::Permission" and (p.get("Principal") == "*" or "FunctionUrlAuthType" in p))
        if public:
            assert res.get("Condition") == "IsPublicEdge", name


def test_function_mode_follows_the_edge_parameter():
    v = R["McpFunction"]["Properties"]["Environment"]["Variables"]
    assert v["DOOR_MODE"] == {"Fn::If": ["IsPublicEdge", "public", "internal"]}
    assert "INTERNAL_INVOKER_ROLE" in v
