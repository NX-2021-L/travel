"""FLY-TSK-031-0A (A-14 debt 2): no lane falls back to a region. The region arrives as a setting (the
Lambda runtime's AWS_REGION); absent, each records-table lane refuses and nothing is read or written.
Offline: the refusal happens before any client is built."""
import sys
from pathlib import Path

import pytest

pytest.importorskip("boto3")
LAMBDA = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(LAMBDA))
import lane_2a_records as l2a  # noqa: E402
import lane_2b_home as l2b  # noqa: E402
import lane_2e_corrections as l2e  # noqa: E402
import settings  # noqa: E402


@pytest.fixture
def no_region(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setenv("RECORDS_TABLE_NAME", "some-records-table")
    monkeypatch.setattr(l2a, "_resource", None)
    monkeypatch.setattr(l2b, "_resource", None)


def test_settings_region_has_no_default(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with pytest.raises(settings.SettingsError) as e:
        settings.region()
    assert "AWS_REGION" in str(e.value)
    assert settings.region({"AWS_DEFAULT_REGION": " r-9 "}) == "r-9"
    assert settings.region({"AWS_REGION": "r-1", "AWS_DEFAULT_REGION": "r-2"}) == "r-1"
    with pytest.raises(settings.SettingsError):
        settings.region({"AWS_REGION": "  "})


def test_lane_2a_refuses_without_a_region(no_region):
    with pytest.raises(l2a._Refuse) as e:
        l2a._table()
    assert "AWS_REGION" in e.value.message and "Nothing was read or written" in e.value.message


def test_lane_2b_refuses_without_a_region(no_region):
    with pytest.raises(l2b._TableUnavailable) as e:
        l2b._table()
    assert "AWS_REGION" in str(e.value)


def test_lane_2e_refuses_without_a_region(no_region):
    with pytest.raises(l2e._Fail) as e:
        l2e._table()
    assert "AWS_REGION" in e.value.message


def test_no_module_names_a_default_region():
    for p in LAMBDA.glob("*.py"):
        assert "us-west-2" not in p.read_text(), p.name
