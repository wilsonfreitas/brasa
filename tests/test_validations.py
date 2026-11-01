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


# ---------------------------------------------------------------------------
# Rule index (Task 2)
# ---------------------------------------------------------------------------

import textwrap  # noqa: E402

import pytest  # noqa: E402
from bizdays import Calendar  # noqa: E402

from brasa.engine.template import clear_template_cache  # noqa: E402
from brasa.engine.validations import collect_rules, validate_datasets  # noqa: E402

_ETL = """\
id: {name}
description: ETL fixture
etl:
  pipeline:
    - step: sql_query
      datasets: [input.b3-bvbg086]
      query: SELECT 1
"""

_INLINE = """\
validations:
  - rule: calendar-completeness
    calendar: ANBIMA
"""

_MULTI = """\
id: val-multi
description: multi-dataset fixture
downloader:
  function: brasa.downloaders.simple_download
  url: http://example.com/x.csv
  format: csv
reader:
  pipeline:
    steps:
      - read_csv
      - apply_fields_multi
writer:
  partitioning: [refdate]
datasets:
  a:
    tag: A
    fields:
      - name: refdate
        description: Reference date
        type: date
    validations:
      - rule: not-null
        columns: [refdate]
"""


@pytest.fixture
def folder(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    (root / "templates").mkdir(parents=True)
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(root))
    clear_template_cache()
    yield root
    clear_template_cache()


def _tpl(root, name, text):
    (root / "templates" / f"{name}.yaml").write_text(text)


def _rules(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


def test_inline_rules_single_dataset_template(folder):
    _tpl(folder, "val-etl", _ETL.format(name="val-etl") + _INLINE)
    index, known, problems = collect_rules()
    assert index["staging.val-etl"] == [
        {"rule": "calendar-completeness", "calendar": "ANBIMA"}
    ]
    assert "staging.val-etl" in known
    assert problems == []


def test_inline_rules_multi_dataset_template(folder):
    _tpl(folder, "val-multi", _MULTI)
    index, known, _ = collect_rules()
    assert index["input.val-multi-a"] == [{"rule": "not-null", "columns": ["refdate"]}]
    assert "input.val-multi-a" in known


def test_top_level_validations_on_multi_dataset_template_is_error(folder):
    _tpl(folder, "val-multi", _MULTI + _INLINE)
    _, _, problems = collect_rules()
    assert len(problems) == 1
    assert problems[0].severity == "error"
    assert "val-multi" in problems[0].description
    assert "datasets" in problems[0].description


def test_folder_rules_file_collected(folder):
    _tpl(folder, "val-etl", _ETL.format(name="val-etl") + _INLINE)
    _rules(
        folder / "validations" / "mine.yaml",
        """
        staging.val-etl:
          - rule: not-null
            columns: [refdate]
        """,
    )
    index, _, problems = collect_rules()
    assert [r["rule"] for r in index["staging.val-etl"]] == [
        "calendar-completeness",
        "not-null",
    ]
    assert problems == []


def test_unknown_dataset_key_in_rules_file_is_error(folder):
    _tpl(folder, "val-etl", _ETL.format(name="val-etl"))
    path = _rules(
        folder / "validations" / "mine.yaml",
        """
        staging.val-typo:
          - rule: not-null
            columns: [refdate]
        """,
    )
    index, _, problems = collect_rules()
    assert "staging.val-typo" not in index
    assert [p.severity for p in problems] == ["error"]
    assert (
        f"{path}: unknown dataset 'staging.val-typo' (no template produces it)"
        in problems[0].description
    )


def test_rules_option_file_merged(folder, tmp_path):
    _tpl(folder, "val-etl", _ETL.format(name="val-etl") + _INLINE)
    extra = _rules(
        tmp_path / "extra.yaml",
        """
        staging.val-etl:
          - rule: no-duplicates
            key: [refdate]
        """,
    )
    index, _, _ = collect_rules(rules_file=extra)
    assert [r["rule"] for r in index["staging.val-etl"]] == [
        "calendar-completeness",
        "no-duplicates",
    ]


def test_validate_unknown_dataset_argument_raises(folder):
    with pytest.raises(ValueError, match="staging.nope"):
        validate_datasets(["staging.nope"])


def test_validate_missing_rules_file_raises(folder, tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_datasets(rules_file=tmp_path / "nope.yaml")


def test_validate_dataset_without_rules_notes(folder):
    _tpl(folder, "val-plain", _ETL.format(name="val-plain"))
    report = validate_datasets(["staging.val-plain"])
    assert [(i.code, i.severity) for i in report.issues] == [("no-rules", "info")]


def test_validate_reports_gap_from_inline_rule(folder):
    _tpl(folder, "val-etl", _ETL.format(name="val-etl") + _INLINE)
    cal = Calendar.load("ANBIMA")
    days = [
        d.date() if hasattr(d, "date") else d
        for d in cal.seq(date(2020, 1, 2), date(2020, 1, 31))
    ]
    _write_series_parquet("staging/val-etl", None, days[:5] + days[6:])
    report = validate_datasets(["staging.val-etl"])
    codes = [i.code for i in report.issues]
    assert codes == ["validation-run", "calendar-completeness"]
    assert str(days[5]) in report.errors()[0].details

    _write_series_parquet("staging/val-etl", None, days)
    report = validate_datasets(["staging.val-etl"])
    assert report.errors() == []
    assert "staging.val-etl: 1 rule(s) checked" in report.issues[0].description
