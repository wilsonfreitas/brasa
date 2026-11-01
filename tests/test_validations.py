"""brasa validate: rule index, visible skips, CLI (WIL-201)."""

from datetime import date

from brasa.engine.validations import run_rules
from tests.test_doctor import _write_series_parquet

_CDI_RULE = {
    "rule": "calendar-completeness",
    "group_column": "symbol",
    "series": {"USD": {"calendar": "ANBIMA"}},
}


def _skips(issues):
    return [i for i in issues if i.code == "validation-skipped"]


def test_absent_dataset_is_reported_as_skipped():
    issues = run_rules({"staging.val-absent": [_CDI_RULE]})
    assert [i.severity for i in issues] == ["info"]
    assert "skipped: staging.val-absent/USD" in issues[0].description
    assert "dataset not in the datalake" in issues[0].description


def test_absent_series_is_reported_as_skipped():
    _write_series_parquet("staging/val-series", "CDI", [date(2020, 1, 2)])
    issues = run_rules({"staging.val-series": [_CDI_RULE]})
    skips = _skips(issues)
    assert len(skips) == 1
    assert "series 'USD' not found" in skips[0].description
    assert not [i for i in issues if i.severity == "error"]


def test_content_rule_on_absent_dataset_is_skipped():
    issues = run_rules(
        {"staging.val-absent2": [{"rule": "not-null", "columns": ["refdate"]}]}
    )
    assert len(_skips(issues)) == 1
    assert issues[0].severity == "info"
