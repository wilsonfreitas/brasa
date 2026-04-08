"""Tests for download plan smart_update support.

Tests cover:
- smart_update in DownloadPlanDefaults
- smart_update in per-task overrides
- smart_update passed to download_marketdata
"""

from unittest.mock import Mock, patch

from brasa.engine.download_plan import (
    DownloadPlan,
    DownloadPlanDefaults,
    DownloadPlanTask,
    _execute_task,
)


class TestDownloadPlanDefaults:
    """Test DownloadPlanDefaults with smart_update."""

    def test_defaults_smart_update_default_false(self):
        """Test that smart_update defaults to False."""
        defaults = DownloadPlanDefaults()
        assert defaults.smart_update is False

    def test_defaults_smart_update_true(self):
        """Test setting smart_update to True."""
        defaults = DownloadPlanDefaults(smart_update=True)
        assert defaults.smart_update is True

    def test_defaults_all_fields(self):
        """Test DownloadPlanDefaults with all fields."""
        defaults = DownloadPlanDefaults(
            refdate="2026-04-01:",
            calendar="ANBIMA",
            force=True,
            smart_update=True,
        )
        assert defaults.refdate == "2026-04-01:"
        assert defaults.calendar == "ANBIMA"
        assert defaults.force is True
        assert defaults.smart_update is True


class TestDownloadPlanTask:
    """Test DownloadPlanTask with smart_update."""

    def test_task_smart_update_none_default(self):
        """Test that task smart_update defaults to None."""
        task = DownloadPlanTask(template="b3-cotahist-daily")
        assert task.smart_update is None

    def test_task_smart_update_override_true(self):
        """Test task-level smart_update override to True."""
        task = DownloadPlanTask(template="b3-cotahist-daily", smart_update=True)
        assert task.smart_update is True

    def test_task_smart_update_override_false(self):
        """Test task-level smart_update override to False."""
        task = DownloadPlanTask(template="b3-cotahist-daily", smart_update=False)
        assert task.smart_update is False


class TestDownloadPlanFromDict:
    """Test parsing smart_update from YAML dict."""

    def test_plan_from_dict_with_defaults_smart_update(self):
        """Test parsing plan with smart_update in defaults."""
        data = {
            "name": "test-plan",
            "defaults": {
                "smart_update": True,
                "calendar": "B3",
            },
            "tasks": [
                {"template": "b3-cotahist-daily"},
            ],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.defaults.smart_update is True
        assert plan.defaults.calendar == "B3"

    def test_plan_from_dict_defaults_smart_update_false(self):
        """Test that smart_update defaults to False when not specified."""
        data = {
            "name": "test-plan",
            "defaults": {"calendar": "B3"},
            "tasks": [
                {"template": "b3-cotahist-daily"},
            ],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.defaults.smart_update is False

    def test_plan_from_dict_with_task_smart_update(self):
        """Test parsing plan with smart_update in task."""
        data = {
            "name": "test-plan",
            "defaults": {"smart_update": False},
            "tasks": [
                {"template": "b3-cotahist-daily", "smart_update": True},
            ],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.defaults.smart_update is False
        assert plan.tasks[0].smart_update is True

    def test_plan_from_dict_multiple_tasks_with_different_smart_update(self):
        """Test plan with different smart_update values per task."""
        data = {
            "name": "test-plan",
            "defaults": {"smart_update": False},
            "tasks": [
                {"template": "b3-cotahist-daily", "smart_update": True},
                {"template": "bcb-sgs"},  # Will inherit defaults (False)
                {"template": "b3-futures-settlement-prices", "smart_update": True},
            ],
        }
        plan = DownloadPlan.from_dict(data)
        assert plan.tasks[0].smart_update is True
        assert plan.tasks[1].smart_update is None  # None means inherit from defaults
        assert plan.tasks[2].smart_update is True


class TestExecuteTaskSmartUpdate:
    """Test _execute_task with smart_update."""

    @patch("brasa.engine.api.download_marketdata")
    def test_execute_task_passes_smart_update_to_download_marketdata(
        self, mock_download
    ):
        """Test that smart_update is passed to download_marketdata."""
        from brasa.engine import Verbosity

        task = DownloadPlanTask(template="b3-cotahist-daily")
        resolved_args = {}
        verbosity = Verbosity.NORMAL

        mock_download.return_value = Mock()

        _execute_task(
            task,
            resolved_args,
            verbosity,
            plan_calendar="B3",
            plan_smart_update=True,
        )

        # Verify download_marketdata was called with smart_update=True
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args[1]
        assert call_kwargs["smart_update"] is True
        assert call_kwargs["calendar"] == "B3"

    @patch("brasa.engine.api.download_marketdata")
    def test_execute_task_task_override_wins(self, mock_download):
        """Test that task-level smart_update overrides plan defaults."""
        from brasa.engine import Verbosity

        task = DownloadPlanTask(template="b3-cotahist-daily", smart_update=False)
        resolved_args = {}
        verbosity = Verbosity.NORMAL

        mock_download.return_value = Mock()

        _execute_task(
            task,
            resolved_args,
            verbosity,
            plan_calendar="B3",
            plan_smart_update=True,
        )

        # Task override (False) should win over plan default (True)
        call_kwargs = mock_download.call_args[1]
        assert call_kwargs["smart_update"] is False

    @patch("brasa.engine.api.download_marketdata")
    def test_execute_task_inherits_plan_default(self, mock_download):
        """Test that task inherits plan smart_update when not overridden."""
        from brasa.engine import Verbosity

        task = DownloadPlanTask(template="b3-cotahist-daily")  # smart_update=None
        resolved_args = {}
        verbosity = Verbosity.NORMAL

        mock_download.return_value = Mock()

        _execute_task(
            task,
            resolved_args,
            verbosity,
            plan_calendar="B3",
            plan_smart_update=True,
        )

        # Task should inherit plan default (True)
        call_kwargs = mock_download.call_args[1]
        assert call_kwargs["smart_update"] is True

    @patch("brasa.engine.api.download_marketdata")
    def test_execute_task_passes_calendar(self, mock_download):
        """Test that calendar is passed to download_marketdata."""
        from brasa.engine import Verbosity

        task = DownloadPlanTask(template="bcb-sgs")
        resolved_args = {}
        verbosity = Verbosity.NORMAL

        mock_download.return_value = Mock()

        _execute_task(
            task,
            resolved_args,
            verbosity,
            plan_calendar="ANBIMA",
            plan_smart_update=False,
        )

        call_kwargs = mock_download.call_args[1]
        assert call_kwargs["calendar"] == "ANBIMA"


class TestDownloadPlanSmartUpdateYAML:
    """Test loading smart_update from YAML files."""

    def test_plan_from_file_with_smart_update(self, tmp_path):
        """Test loading a plan file with smart_update."""
        plan_file = tmp_path / "smart-update-plan.yaml"
        plan_file.write_text(
            """
name: daily-smart-update
defaults:
  smart_update: true
  calendar: B3
tasks:
  - template: b3-cotahist-daily
  - template: b3-futures-settlement-prices
  - template: b3-indexes-composition
    smart_update: false
"""
        )

        plan = DownloadPlan.from_file(plan_file)
        assert plan.defaults.smart_update is True
        assert plan.tasks[0].smart_update is None  # Inherit default
        assert plan.tasks[1].smart_update is None  # Inherit default
        assert plan.tasks[2].smart_update is False  # Override default
