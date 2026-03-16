"""Tests for brasa.engine.download_plan.

All tests are unit tests — no network access required.
download_marketdata and get_symbols are mocked throughout.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from brasa.engine.download_plan import (
    DownloadPlan,
    DownloadPlanReport,
    execute_download_plan,
    resolve_plan_args,
)
from brasa.engine.reporting import TaskReport, TaskResult, TaskStatus, Verbosity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task_report(
    template: str,
    statuses: list[TaskStatus] | None = None,
) -> TaskReport:
    """Build a minimal TaskReport with the given statuses."""
    report = TaskReport(
        operation="download",
        template_name=template,
        verbosity=Verbosity.QUIET,
    )
    report.start(total=len(statuses or []))
    for status in statuses or []:
        result = TaskResult(
            status=status,
            operation="download",
            template_name=template,
        )
        report.add_result(result)
    report.finish()
    return report


VALID_PLAN_DICT = {
    "name": "test-plan",
    "description": "A test plan",
    "defaults": {"refdate": "2026-01", "calendar": "B3", "reprocess": False},
    "tasks": [
        {"template": "tmpl-a"},
        {"template": "tmpl-b", "args": {"extra": "val"}, "reprocess": True},
    ],
}


# ---------------------------------------------------------------------------
# 1. Plan parsing
# ---------------------------------------------------------------------------


class TestDownloadPlanFromDict:
    def test_valid_plan(self):
        plan = DownloadPlan.from_dict(VALID_PLAN_DICT)
        assert plan.name == "test-plan"
        assert plan.description == "A test plan"
        assert plan.defaults.refdate == "2026-01"
        assert plan.defaults.calendar == "B3"
        assert plan.defaults.reprocess is False
        assert len(plan.tasks) == 2

    def test_task_fields(self):
        plan = DownloadPlan.from_dict(VALID_PLAN_DICT)
        assert plan.tasks[0].template == "tmpl-a"
        assert plan.tasks[0].args == {}
        assert plan.tasks[0].reprocess is False
        assert plan.tasks[1].template == "tmpl-b"
        assert plan.tasks[1].args == {"extra": "val"}
        assert plan.tasks[1].reprocess is True

    def test_missing_name_raises(self):
        data = {**VALID_PLAN_DICT}
        del data["name"]
        with pytest.raises(ValueError, match="'name'"):
            DownloadPlan.from_dict(data)

    def test_missing_tasks_raises(self):
        data = {**VALID_PLAN_DICT, "tasks": []}
        with pytest.raises(ValueError, match="'tasks'"):
            DownloadPlan.from_dict(data)

    def test_task_without_template_raises(self):
        data = {**VALID_PLAN_DICT, "tasks": [{"args": {}}]}
        with pytest.raises(ValueError, match="'template'"):
            DownloadPlan.from_dict(data)

    def test_defaults_optional(self):
        data = {"name": "minimal", "tasks": [{"template": "tmpl-a"}]}
        plan = DownloadPlan.from_dict(data)
        assert plan.defaults.refdate is None
        assert plan.defaults.calendar == "B3"
        assert plan.defaults.reprocess is False

    def test_task_inherits_default_reprocess(self):
        data = {
            "name": "p",
            "defaults": {"reprocess": True},
            "tasks": [{"template": "tmpl-a"}],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.tasks[0].reprocess is True

    def test_task_overrides_default_reprocess(self):
        data = {
            "name": "p",
            "defaults": {"reprocess": True},
            "tasks": [{"template": "tmpl-a", "reprocess": False}],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.tasks[0].reprocess is False


class TestDownloadPlanFromFile:
    def test_load_valid_file(self, tmp_path):
        import yaml

        plan_file = tmp_path / "plan.yaml"
        plan_file.write_text(yaml.dump(VALID_PLAN_DICT))
        plan = DownloadPlan.from_file(plan_file)
        assert plan.name == "test-plan"

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            DownloadPlan.from_file(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_raises(self, tmp_path):
        plan_file = tmp_path / "plan.yaml"
        plan_file.write_text("- just a list\n- not a mapping\n")
        with pytest.raises(ValueError, match="expected a mapping"):
            DownloadPlan.from_file(plan_file)


# ---------------------------------------------------------------------------
# 2. Plan validation
# ---------------------------------------------------------------------------


class TestDownloadPlanValidate:
    def test_valid_templates(self):
        plan = DownloadPlan.from_dict(VALID_PLAN_DICT)
        with patch(
            "brasa.engine.download_plan.list_templates",
            return_value=["tmpl-a", "tmpl-b"],
        ):
            errors = plan.validate()
        assert errors == []

    def test_unknown_template(self):
        plan = DownloadPlan.from_dict(VALID_PLAN_DICT)
        with patch(
            "brasa.engine.download_plan.list_templates",
            return_value=["tmpl-a"],
        ):
            errors = plan.validate()
        assert len(errors) == 1
        assert "tmpl-b" in errors[0]

    def test_all_unknown(self):
        plan = DownloadPlan.from_dict(VALID_PLAN_DICT)
        with patch(
            "brasa.engine.download_plan.list_templates",
            return_value=[],
        ):
            errors = plan.validate()
        assert len(errors) == 2


# ---------------------------------------------------------------------------
# 3. Argument resolution
# ---------------------------------------------------------------------------


class TestResolvePlanArgs:
    def test_passthrough_string(self):
        assert resolve_plan_args({"key": "value"}) == {"key": "value"}

    def test_passthrough_int(self):
        assert resolve_plan_args({"n": 42}) == {"n": 42}

    def test_passthrough_list(self):
        assert resolve_plan_args({"items": [1, 2, 3]}) == {"items": [1, 2, 3]}

    def test_integer_range(self):
        result = resolve_plan_args({"year": "2020:2023"})
        assert result["year"] == [2020, 2021, 2022, 2023]

    def test_symbols_prefix(self):
        with patch(
            "brasa.queries.get_symbols",
            return_value=["IBOV", "IFIX"],
        ) as mock_gs:
            result = resolve_plan_args({"index": "symbols:index"})
        mock_gs.assert_called_once_with("index")
        assert result["index"] == ["IBOV", "IFIX"]

    def test_empty_args(self):
        assert resolve_plan_args({}) == {}


# ---------------------------------------------------------------------------
# 4. Refdate smart injection
# ---------------------------------------------------------------------------


class TestTemplateRequiresRefdate:
    def _mock_template(self, has_refdate: bool):
        tmpl = MagicMock()
        tmpl.downloader.args = {"refdate": None} if has_refdate else {}
        return tmpl

    def test_requires_refdate(self):
        from brasa.engine.download_plan import _template_requires_refdate

        with patch(
            "brasa.engine.download_plan.retrieve_template",
            return_value=self._mock_template(True),
        ):
            assert _template_requires_refdate("tmpl-a") is True

    def test_does_not_require_refdate(self):
        from brasa.engine.download_plan import _template_requires_refdate

        with patch(
            "brasa.engine.download_plan.retrieve_template",
            return_value=self._mock_template(False),
        ):
            assert _template_requires_refdate("tmpl-a") is False

    def test_returns_false_on_exception(self):
        from brasa.engine.download_plan import _template_requires_refdate

        with patch(
            "brasa.engine.download_plan.retrieve_template",
            side_effect=Exception("not found"),
        ):
            assert _template_requires_refdate("tmpl-a") is False


# ---------------------------------------------------------------------------
# 5. Plan execution
# ---------------------------------------------------------------------------


def _make_plan(tasks_data=None, defaults=None):
    """Create a DownloadPlan using mocked list_templates."""
    data = {
        "name": "exec-plan",
        "defaults": defaults or {},
        "tasks": tasks_data or [{"template": "tmpl-a"}, {"template": "tmpl-b"}],
    }
    with patch(
        "brasa.engine.download_plan.list_templates",
        return_value=["tmpl-a", "tmpl-b"],
    ):
        return DownloadPlan.from_dict(data)


class TestExecuteDownloadPlan:
    def _run(self, plan, reports_by_template, refdate_override=None):
        """Run execute_download_plan with mocked download_marketdata."""

        def fake_download(template_name, force=False, verbosity=..., **kwargs):
            return reports_by_template.get(
                template_name,
                _make_task_report(template_name, [TaskStatus.PASSED]),
            )

        with (
            patch(
                "brasa.engine.api.download_marketdata",
                side_effect=fake_download,
            ),
            patch(
                "brasa.engine.download_plan._template_requires_refdate",
                return_value=False,
            ),
        ):
            return execute_download_plan(
                plan,
                refdate_override=refdate_override,
                verbosity=Verbosity.QUIET,
            )

    def test_all_tasks_executed(self):
        plan = _make_plan()
        result = self._run(plan, {})
        assert set(result.task_reports.keys()) == {"tmpl-a", "tmpl-b"}

    def test_success_when_all_pass(self):
        plan = _make_plan()
        result = self._run(plan, {})
        assert result.success is True

    def test_failure_detected(self):
        plan = _make_plan()
        reports = {
            "tmpl-a": _make_task_report("tmpl-a", [TaskStatus.FAILED]),
        }
        result = self._run(plan, reports)
        assert result.success is False

    def test_continue_on_error(self):
        """One failing task must not stop the others."""
        plan = _make_plan()
        call_log = []

        def fake_download(template_name, force=False, verbosity=..., **kwargs):
            call_log.append(template_name)
            if template_name == "tmpl-a":
                raise RuntimeError("boom")
            return _make_task_report(template_name, [TaskStatus.PASSED])

        with (
            patch(
                "brasa.engine.api.download_marketdata",
                side_effect=fake_download,
            ),
            patch(
                "brasa.engine.download_plan._template_requires_refdate",
                return_value=False,
            ),
        ):
            result = execute_download_plan(plan, verbosity=Verbosity.QUIET)

        assert "tmpl-a" in result.task_reports
        assert "tmpl-b" in result.task_reports
        assert "tmpl-b" in call_log

    def test_refdate_injected_when_required(self):
        """refdate_override should be passed to templates that need it."""
        plan = _make_plan()
        captured = {}

        def fake_download(template_name, force=False, verbosity=..., **kwargs):
            captured[template_name] = kwargs
            return _make_task_report(template_name, [TaskStatus.PASSED])

        fake_dates = ["2026-01-02"]
        with (
            patch(
                "brasa.engine.api.download_marketdata",
                side_effect=fake_download,
            ),
            patch(
                "brasa.engine.download_plan._template_requires_refdate",
                return_value=True,
            ),
        ):
            execute_download_plan(
                plan,
                refdate_override=fake_dates,
                verbosity=Verbosity.QUIET,
            )

        assert captured["tmpl-a"].get("refdate") == fake_dates

    def test_refdate_not_injected_when_not_required(self):
        plan = _make_plan()
        captured = {}

        def fake_download(template_name, force=False, verbosity=..., **kwargs):
            captured[template_name] = kwargs
            return _make_task_report(template_name, [TaskStatus.PASSED])

        with (
            patch(
                "brasa.engine.api.download_marketdata",
                side_effect=fake_download,
            ),
            patch(
                "brasa.engine.download_plan._template_requires_refdate",
                return_value=False,
            ),
        ):
            execute_download_plan(
                plan,
                refdate_override=["2026-01-02"],
                verbosity=Verbosity.QUIET,
            )

        assert "refdate" not in captured["tmpl-a"]

    def test_total_duration_positive(self):
        plan = _make_plan()
        result = self._run(plan, {})
        assert result.total_duration >= 0.0


# ---------------------------------------------------------------------------
# 6. Report properties
# ---------------------------------------------------------------------------


class TestDownloadPlanReport:
    def test_success_true_when_all_pass(self):
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        assert report.success is True

    def test_success_false_on_error(self):
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.ERROR])
        assert report.success is False

    def test_success_false_on_failed(self):
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.FAILED])
        assert report.success is False

    def test_total_duration(self):
        from datetime import timedelta

        report = DownloadPlanReport(plan_name="p")
        report._start_time = report._end_time = None
        assert report.total_duration == 0.0

        now = __import__("datetime").datetime.now()
        report._start_time = now
        report._end_time = now + timedelta(seconds=5)
        assert abs(report.total_duration - 5.0) < 0.01

    def test_summary_contains_plan_name(self):
        report = DownloadPlanReport(plan_name="my-plan")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        summary = report.summary()
        assert "my-plan" in summary

    def test_summary_contains_template_name(self):
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        summary = report.summary()
        assert "tmpl-a" in summary


# ---------------------------------------------------------------------------
# 6b. Implicit dependency reports
# ---------------------------------------------------------------------------


class TestImplicitDependencyReports:
    def test_summary_shows_implicit_dep(self):
        """[auto] section appears when implicit_task_reports is populated."""
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        dep_report = _make_task_report("dep-etl", [TaskStatus.PASSED])
        report.implicit_task_reports["dep-etl"] = dep_report
        summary = report.summary()
        assert "[auto]" in summary
        assert "dep-etl" in summary

    def test_summary_no_auto_section_when_no_implicit(self):
        """[auto] section is absent when implicit_task_reports is empty."""
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        summary = report.summary()
        assert "[auto]" not in summary

    def test_summary_totals_include_auto_count(self):
        """Totals line shows ', N auto' when implicit reports are present."""
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        dep_report = _make_task_report("dep-etl", [TaskStatus.PASSED])
        report.implicit_task_reports["dep-etl"] = dep_report
        summary = report.summary()
        assert "1 auto" in summary

    def test_success_false_when_implicit_dep_fails(self):
        """success is False when an implicit report has FAILED results."""
        report = DownloadPlanReport(plan_name="p")
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        dep_report = _make_task_report("dep-etl", [TaskStatus.FAILED])
        report.implicit_task_reports["dep-etl"] = dep_report
        assert report.success is False

    def test_execute_plan_collects_implicit_reports(self):
        """Dependency reports on a TaskReport land in implicit_task_reports."""
        plan = _make_plan(tasks_data=[{"template": "tmpl-a"}])

        dep_report = _make_task_report("dep-etl", [TaskStatus.PASSED])

        def fake_execute_task(task, resolved_args, verbosity):
            task_report = _make_task_report(task.template, [TaskStatus.PASSED])
            task_report.dependency_reports = [dep_report]
            return task_report

        with (
            patch(
                "brasa.engine.download_plan._execute_task",
                side_effect=fake_execute_task,
            ),
        ):
            result = execute_download_plan(plan, verbosity=Verbosity.QUIET)

        assert "dep-etl" in result.implicit_task_reports
        assert result.implicit_task_reports["dep-etl"] is dep_report

    def test_implicit_report_not_duplicated_across_tasks(self):
        """The same dependency template is only stored once in implicit_task_reports."""
        plan = _make_plan(tasks_data=[{"template": "tmpl-a"}, {"template": "tmpl-b"}])

        dep_report = _make_task_report("dep-etl", [TaskStatus.PASSED])

        def fake_execute_task(task, resolved_args, verbosity):
            task_report = _make_task_report(task.template, [TaskStatus.PASSED])
            task_report.dependency_reports = [dep_report]
            return task_report

        with (
            patch(
                "brasa.engine.download_plan._execute_task",
                side_effect=fake_execute_task,
            ),
        ):
            result = execute_download_plan(plan, verbosity=Verbosity.QUIET)

        # Only one entry for dep-etl even though two tasks each produced it
        assert list(result.implicit_task_reports.keys()).count("dep-etl") == 1


# ---------------------------------------------------------------------------
# 7. Report saving
# ---------------------------------------------------------------------------


class TestDownloadPlanReportSave:
    def _make_report(self) -> DownloadPlanReport:
        from datetime import datetime, timedelta

        report = DownloadPlanReport(plan_name="save-test")
        report._start_time = datetime.now()
        report._end_time = report._start_time + timedelta(seconds=1)
        report.task_reports["tmpl-a"] = _make_task_report("tmpl-a", [TaskStatus.PASSED])
        return report

    def test_save_json(self, tmp_path):
        report = self._make_report()
        out = tmp_path / "report.json"
        report.save_report(out, format="json")
        data = json.loads(out.read_text())
        assert data["plan_name"] == "save-test"
        assert "tasks" in data
        assert data["tasks"][0]["template"] == "tmpl-a"

    def test_save_txt(self, tmp_path):
        report = self._make_report()
        out = tmp_path / "report.txt"
        report.save_report(out, format="txt")
        text = out.read_text()
        assert "save-test" in text

    def test_save_format_inferred_from_extension_json(self, tmp_path):
        report = self._make_report()
        out = tmp_path / "report.json"
        report.save_report(out)  # format defaults to json
        data = json.loads(out.read_text())
        assert "plan_name" in data

    def test_save_format_inferred_txt_for_non_json(self, tmp_path):
        report = self._make_report()
        out = tmp_path / "report.txt"
        report.save_report(out, format="txt")
        assert "PLAN SUMMARY" in out.read_text()


# ---------------------------------------------------------------------------
# 8. CLI argument parsing
# ---------------------------------------------------------------------------


class TestCliDownloadPlanArgs:
    """Test that the CLI correctly handles --plan and template mutual exclusion."""

    def _parse(self, argv: list[str]):
        """Run argparse on the given argv and return the namespace."""

        # Reload cli module to get a fresh parser state
        import brasa.cli as cli_mod

        return cli_mod.parser.parse_args(argv)

    def test_plan_flag_parsed(self):
        ns = self._parse(["download", "--plan", "my-plan.yaml"])
        assert ns.plan == "my-plan.yaml"
        assert ns.template == []

    def test_template_names_parsed(self):
        ns = self._parse(["download", "tmpl-a", "tmpl-b"])
        assert ns.template == ["tmpl-a", "tmpl-b"]
        assert ns.plan is None

    def test_plan_with_date_override(self):
        ns = self._parse(["download", "--plan", "p.yaml", "--arg", "refdate=@2026-01"])
        assert ns.plan == "p.yaml"
        assert ns.arg == ["refdate=@2026-01"]
