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


INTERNAL_GRANTS = {"InternalInvokerInvoke", "InternalDeployerInvoke"}


def test_function_url_and_permission_only_for_public_edge():
    assert T["Conditions"]["IsPublicEdge"] == {"Fn::Equals": [{"Ref": "PublicEdge"}, "true"]}
    assert T["Parameters"]["PublicEdge"]["Default"] == "false"
    guarded = {k for k, v in R.items() if v.get("Condition") == "IsPublicEdge"}
    assert guarded == {"FunctionUrl", "FunctionUrlPublicInvoke"}
    assert R["FunctionUrl"]["Type"] == "AWS::Lambda::Url"
    assert R["FunctionUrlPublicInvoke"]["Type"] == "AWS::Lambda::Permission"
    # no other resource of either type exists unguarded; the two internal-door grants (FLY-TSK-028-0D) are
    # scoped to a named role and are absent for a public edge (asserted in the tests below)
    for k, v in R.items():
        if v["Type"] in ("AWS::Lambda::Url", "AWS::Lambda::Permission") and k not in INTERNAL_GRANTS:
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


# ---- FLY-TSK-028-0D: the explicit InvokeFunction grants are the authorization boundary for an internal function ----
def test_internal_invoke_grants_are_scoped_to_the_named_roles_only():
    for name, param in (("InternalInvokerInvoke", "InternalInvokerRole"), ("InternalDeployerInvoke", "InternalDeployRole")):
        res = R[name]
        p = res["Properties"]
        assert res["Type"] == "AWS::Lambda::Permission"
        assert p["Action"] == "lambda:InvokeFunction"
        assert p["Principal"] == {"Ref": param}  # one named role, never a wildcard
        assert "FunctionUrlAuthType" not in p
        cond = T["Conditions"][res["Condition"]]
        assert cond["Fn::And"][0] == {"Fn::Not": [{"Fn::Condition": "IsPublicEdge"}]}  # absent for a public edge
        assert T["Parameters"][param]["Default"] == ""  # an unset role grants nobody


def test_no_invoke_permission_has_a_wildcard_principal_outside_the_public_edge():
    for name, res in R.items():
        if res["Type"] == "AWS::Lambda::Permission" and res["Properties"].get("Principal") == "*":
            assert res.get("Condition") == "IsPublicEdge", name
    assert R["FunctionUrlPublicInvoke"]["Properties"]["Principal"] == "*" and "InternalInvokerInvoke" not in {
        k for k, v in R.items() if v.get("Condition") == "IsPublicEdge"}


# ---- FLY-TSK-031-0A, A-14 (lane 3C): with the public edge off, NO internal function has a public grant ----
def _public_grants(resources, public_edge, conditions=None):
    """Names of resources, created when PublicEdge == public_edge, that grant the function to the public:
    a Function URL, or a permission whose principal is `*` or that names a Function URL auth type. Extends
    the 0D loader and _created evaluator above; there is no second parser."""
    conditions = T["Conditions"] if conditions is None else conditions
    out = []
    for name, res in resources.items():
        cond = res.get("Condition")
        # IsPublicEdge is evaluated; any other condition (the internal grants' GrantsInvoker/GrantsDeployer)
        # is conservatively treated as "may be created", so a public grant behind it is still caught.
        gated = bool(cond) and conditions[cond] == {"Fn::Equals": [{"Ref": "PublicEdge"}, "true"]}
        created = (public_edge == "true") if gated else True
        p = res.get("Properties", {})
        public = res["Type"] == "AWS::Lambda::Url" or (
            res["Type"] == "AWS::Lambda::Permission" and (p.get("Principal") in ("*", {"AWS": "*"}) or "FunctionUrlAuthType" in p))
        if created and public:
            out.append(name)
    return out


def test_with_the_public_edge_off_no_function_url_and_no_public_invoke_permission_is_created():
    assert T["Parameters"]["PublicEdge"]["Default"] == "false"
    assert _public_grants(R, "false") == []
    assert sorted(_public_grants(R, "true")) == ["FunctionUrl", "FunctionUrlPublicInvoke"]  # and the switch is real
    assert _created("FunctionUrl", "false") is False and _created("FunctionUrlPublicInvoke", "false") is False


def test_an_internal_deployment_leaks_no_public_grant_through_outputs_or_other_resource_types():
    assert T["Outputs"]["FunctionUrl"]["Condition"] == "IsPublicEdge"
    for name, out in T["Outputs"].items():
        if "Url" in name or "Url" in json.dumps(out.get("Value", "")):
            assert out.get("Condition") == "IsPublicEdge", name
    # the only ways a Lambda becomes publicly callable: a URL, a permission, or a gateway in front of it
    types = {v["Type"] for v in R.values()}
    assert not {t for t in types if t.startswith(("AWS::ApiGateway", "AWS::CloudFront", "AWS::ElasticLoadBalancingV2"))}
    unconditional = [k for k, v in R.items() if v["Type"] in ("AWS::Lambda::Url", "AWS::Lambda::Permission") and not v.get("Condition")]
    assert unconditional == []
    # every permission that exists with the edge off names one role by reference, never a wildcard (FLY-TSK-028-0D)
    for name, v in R.items():
        if v["Type"] == "AWS::Lambda::Permission" and name != "FunctionUrlPublicInvoke":
            assert v["Properties"]["Principal"] != "*" and "Ref" in v["Properties"]["Principal"], name


def test_the_public_grant_detector_catches_an_unguarded_grant():
    """Negative control: strip the condition from the real template's grants and the detector must see them."""
    stripped = json.loads(json.dumps(R))
    for k in ("FunctionUrl", "FunctionUrlPublicInvoke"):
        del stripped[k]["Condition"]
    assert sorted(_public_grants(stripped, "false")) == ["FunctionUrl", "FunctionUrlPublicInvoke"]
    star = json.loads(json.dumps(R))
    star["Sneaky"] = {"Type": "AWS::Lambda::Permission", "Properties": {"Action": "lambda:InvokeFunction", "Principal": "*"}}
    assert _public_grants(star, "false") == ["Sneaky"]
