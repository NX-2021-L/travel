"""MOM-TSK-007-0I (lane N6I): io's travel names finance's residence-read function (one direct invocation, one grant).
Offline. Test account id is the made-up 111122223333; no account id is added to the repository."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "backend" / "lambda" / "travel_mcp"))
import load_deployment_settings as lds  # noqa: E402
import resolve_env  # noqa: E402
import settings  # noqa: E402
from test_travel_deployment_template import load  # noqa: E402

IO = lds.parse((ROOT / "deploy/settings.env").read_text())
HER = lds.parse((ROOT / "deploy/targets/nana.env").read_text())
FN = "finance-residence-read"


def test_io_s_travel_names_the_residence_read_function_by_bare_name():
    assert IO["FINANCE_READ_FUNCTION"] == FN
    assert "/" not in IO["FINANCE_READ_FUNCTION"] and ":" not in IO["FINANCE_READ_FUNCTION"]  # no ARN, no URL, no account id


def test_the_setting_line_and_its_comment_carry_no_account_id():
    text = (ROOT / "deploy/settings.env").read_text()
    block = text[text.index("MOM-TSK-007-0I"):text.index("FINANCE_READ_FUNCTION=" + FN) + 40]
    assert re.search(r"(?<!\d)\d{12}(?!\d)", block) is None


def test_io_s_file_is_accepted_and_the_name_passes_the_deploy_guard():
    assert not settings.finance_function_problem(IO["FINANCE_READ_FUNCTION"])
    assert "function name or ARN" not in (resolve_env.guard({"FINANCE_READ_FUNCTION": IO["FINANCE_READ_FUNCTION"]}) or "")


def test_a_url_is_still_refused_so_the_https_path_cannot_come_back():
    assert "function name or ARN" in resolve_env.guard({"FINANCE_READ_FUNCTION": "https://finance.example/mcp"})


def test_no_other_io_setting_changed():
    assert {k: IO[k] for k in ("TABLE_NAME", "RECORDS_TABLE_NAME", "SURFACE_NAME", "DOOR_MODE", "COGNITO_REGION",
                               "PUBLIC_BASE_URL", "CORS_ORIGINS", "FUNCTION_NAME", "AWS_REGION")} == {
        "TABLE_NAME": "io-travel-flights", "RECORDS_TABLE_NAME": "io-travel-records", "SURFACE_NAME": "io-travel",
        "DOOR_MODE": "public", "COGNITO_REGION": "us-east-1", "PUBLIC_BASE_URL": "https://travel.thepup.io",
        "CORS_ORIGINS": "https://think.thepup.io", "FUNCTION_NAME": "io-travel-mcp-server", "AWS_REGION": "us-west-2"}
    assert HER["FINANCE_READ_FUNCTION"] == FN  # hers is untouched, and names the same function in her own account


def test_the_template_grants_one_action_on_one_function_for_a_bare_name():
    t = load()
    pol = [p for p in t["Resources"]["ExecutionRole"]["Properties"]["Policies"]
           if isinstance(p, dict) and "Fn::If" in p and p["Fn::If"][0] == "HasFinanceFunction"][0]["Fn::If"][1]
    assert pol["PolicyName"] == "travel-finance-residence-invoke"
    (st,) = pol["PolicyDocument"]["Statement"]
    assert st["Action"] == "lambda:InvokeFunction"
    res = st["Resource"]["Fn::If"]
    assert res[0] == "FinanceReadIsArn"
    assert "function:${FinanceReadFunction}" in res[2]["Fn::Sub"]  # a bare name: this account, this region, that one function


def test_amendment_a3_io_s_failure_returns_hers_too_and_hers_returns_hers_alone():
    wf = (ROOT / ".github/workflows/lambda-travel-mcp-deploy.yml").read_text()
    release = wf[wf.index("\n  release:"):wf.index("\n  nana:")]
    nana = wf[wf.index("\n  nana:"):wf.index("\n  rollback:")]
    assert "needs: release" in nana  # io's failure: hers is never started, so hers stays on its previous release
    assert "release_ops.py restore" in release  # io's own undo
    assert "release_ops.py restore" in nana  # hers failing restores hers alone, under her own role
    assert "io's release is NOT undone" in nana
