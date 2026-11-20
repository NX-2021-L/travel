"""MOM-TSK-028: nothing io-named reaches her deployment. Her rendered template, her stack parameters and her target
file carry no IOGRAPH, iograph or jreese (case-insensitive); the function reads the neutral second-issuer name first."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "backend" / "lambda"))
import load_deployment_settings as lds  # noqa: E402
import target_ops  # noqa: E402

WORDS = re.compile(r"iograph|jreese", re.I)
TEMPLATE = ROOT / "infra" / "cloudformation" / "travel-deployment.yaml"
HERS = lds.parse((ROOT / "deploy" / "targets" / "nana.env").read_text())


def _params():
    env = {**HERS, "TARGET_ACCOUNT_ID": "000000000000"}
    return target_ops.stack_parameters(env)


def test_her_template_has_no_io_word():
    hits = [m.group(0) for m in WORDS.finditer(TEMPLATE.read_text())]
    assert hits == [], hits


def test_her_stack_parameters_carry_no_io_word():
    text = " ".join(f"{p['ParameterKey']}={p['ParameterValue']}" for p in _params())
    assert WORDS.search(text) is None, text


def test_her_target_file_has_no_io_word():
    assert WORDS.search((ROOT / "deploy" / "targets" / "nana.env").read_text()) is None


def test_her_function_env_names_have_no_io_word():
    text = TEMPLATE.read_text()
    env_block = text[text.index("Variables:"):]
    names = re.findall(r"^\s{10}([A-Z][A-Z0-9_]+):", env_block, re.M)
    assert "SECOND_ISSUER_ALLOWED_SUBS" in names
    assert [n for n in names if WORDS.search(n)] == []
