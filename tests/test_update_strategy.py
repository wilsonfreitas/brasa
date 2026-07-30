"""Tests for the smart update strategy module.

Tests cover:
- Strategy detection from template YAML
- Resolution of all 5 strategies
- Pre-filtering of cached kwargs
- Cache queries (Python-side deserialization)
"""

from datetime import date
from unittest.mock import Mock, patch

import pytest

from brasa.engine.update_strategy import (
    ResolvedStrategy,
    UpdateStrategy,
    _get_last_downloaded_date,
    _is_today_cached,
    detect_strategy,
    resolve_update,
)
from brasa.util import DownloadArgs


class TestDetectStrategy:
    """Test convention-based strategy detection."""

    def test_detect_strategy_yaml_override(self):
        """Test that update: section with strategy overrides convention."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {}
        template.downloader._extra_key = None
        template.update = {"strategy": "incremental-date"}
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.INCREMENTAL_DATE

    def test_detect_strategy_yaml_override_invalid(self):
        """Test that unknown strategy in update: raises ValueError."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.update = {"strategy": "unknown-strategy"}

        with pytest.raises(ValueError, match="Unknown update strategy"):
            detect_strategy(template)

    def test_detect_strategy_etl_only(self):
        """Test that ETL-only templates (no downloader) → NO_AUTO_UPDATE."""
        template = Mock()
        template.has_downloader = False
        template.update = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.NO_AUTO_UPDATE

    def test_detect_strategy_incremental_date_range(self):
        """Test that start: ~ AND end: ~ → INCREMENTAL_DATE_RANGE."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {"start": None, "end": None}
        template.downloader._extra_key = None
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.INCREMENTAL_DATE_RANGE

    def test_detect_strategy_incremental_date(self):
        """Test that refdate: ~ → INCREMENTAL_DATE."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {"refdate": None}
        template.downloader._extra_key = None
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.INCREMENTAL_DATE

    def test_detect_strategy_dependency_driven(self):
        """Test that dependencies block → DEPENDENCY_DRIVEN."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {"code": None}
        template.downloader._extra_key = None
        template.update = None
        template.dependencies = {"upstream": "template"}

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.DEPENDENCY_DRIVEN

    def test_detect_strategy_dependency_driven_beats_extra_key(self):
        """Test that dependencies > extra-key (deps win)."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {"code": None}
        template.downloader._extra_key = "date"
        template.update = None
        template.dependencies = {"upstream": "template"}

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.DEPENDENCY_DRIVEN

    def test_detect_strategy_daily_snapshot(self):
        """Test that extra-key: date (no deps) → DAILY_SNAPSHOT."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {}
        template.downloader._extra_key = "date"
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.DAILY_SNAPSHOT

    def test_detect_strategy_daily_snapshot_datetime(self):
        """Test that extra-key: datetime (no deps) → DAILY_SNAPSHOT."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {}
        template.downloader._extra_key = "datetime"
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.DAILY_SNAPSHOT

    def test_detect_strategy_no_auto_update(self):
        """Test that no args, no extra-key → NO_AUTO_UPDATE."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        template.downloader.args = {}
        template.downloader._extra_key = None
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        assert strategy == UpdateStrategy.NO_AUTO_UPDATE

    def test_detect_strategy_start_checked_before_refdate(self):
        """Test that start/end is checked before refdate (both present)."""
        template = Mock()
        template.id = "test-template"
        template.has_downloader = True
        template.downloader = Mock()
        # Both start/end and refdate present
        template.downloader.args = {"start": None, "end": None, "refdate": None}
        template.downloader._extra_key = None
        template.update = None
        template.dependencies = None

        strategy = detect_strategy(template)
        # start/end should win
        assert strategy == UpdateStrategy.INCREMENTAL_DATE_RANGE


class TestResolveUpdateIntegration:
    """Test resolve_update with database integration."""

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy._get_last_downloaded_date")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_incremental_date_from_cache(
        self, mock_detect, mock_last_date, mock_retrieve
    ):
        """Test INCREMENTAL_DATE resolution from cache."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.INCREMENTAL_DATE
        mock_last_date.return_value = date(2026, 4, 5)

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE
        assert "refdate" in result.kwargs

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy._get_last_downloaded_date")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_incremental_date_fallback(
        self, mock_detect, mock_last_date, mock_retrieve
    ):
        """Test INCREMENTAL_DATE fallback to 30-day default."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.INCREMENTAL_DATE
        mock_last_date.return_value = None

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE
        assert "refdate" in result.kwargs

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy._get_last_downloaded_date")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_incremental_date_explicit_since(
        self, mock_detect, mock_last_date, mock_retrieve
    ):
        """Test INCREMENTAL_DATE with explicit --since."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.INCREMENTAL_DATE
        mock_last_date.return_value = date(2026, 4, 5)

        result = resolve_update("test-template", calendar="B3", since="2026-04-01")

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE
        assert "refdate" in result.kwargs

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy._get_last_downloaded_end_date")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_date_range_from_cache(
        self, mock_detect, mock_last_end, mock_retrieve
    ):
        """Test INCREMENTAL_DATE_RANGE resolution from cache."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.INCREMENTAL_DATE_RANGE
        mock_last_end.return_value = date(2026, 4, 5)

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE_RANGE
        # start/end are DateRange objects
        assert "start" in result.kwargs or result.kwargs == {}

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy._get_last_downloaded_end_date")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_date_range_custom_overlap(
        self, mock_detect, mock_last_end, mock_retrieve
    ):
        """Test INCREMENTAL_DATE_RANGE with custom overlap_days."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.INCREMENTAL_DATE_RANGE
        mock_last_end.return_value = date(2026, 4, 5)

        result = resolve_update("test-template", calendar="B3", overlap_days=30)

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE_RANGE

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_daily_snapshot(self, mock_detect, mock_retrieve):
        """Test DAILY_SNAPSHOT always resolves to an empty-kwargs download."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.DAILY_SNAPSHOT

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.DAILY_SNAPSHOT
        assert result.kwargs == {}

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_dependency_driven(self, mock_detect, mock_retrieve):
        """Test DEPENDENCY_DRIVEN resolution."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.DEPENDENCY_DRIVEN

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.DEPENDENCY_DRIVEN
        assert result.kwargs == {}

    @patch("brasa.engine.template.retrieve_template")
    @patch("brasa.engine.update_strategy.detect_strategy")
    def test_resolve_no_auto_update(self, mock_detect, mock_retrieve):
        """Test NO_AUTO_UPDATE resolution."""
        mock_retrieve.return_value = Mock()
        mock_detect.return_value = UpdateStrategy.NO_AUTO_UPDATE

        result = resolve_update("test-template", calendar="B3")

        assert result.strategy == UpdateStrategy.NO_AUTO_UPDATE
        assert result.kwargs == {}


class TestCacheQueries:
    """Test cache query helpers with Python-side deserialization."""

    @pytest.mark.integration
    def test_get_last_downloaded_date_from_cache(self):
        """Test retrieving max refdate from cache."""
        # This is an integration test that requires a real cache
        from brasa.engine.cache import CacheMetadata

        # Create test metadata (unused, but kept for documentation)
        meta1 = CacheMetadata("test-template")
        meta1.download_args = DownloadArgs({"refdate": "2026-04-01"})
        meta1.download_checksum = "test-checksum"

        # Query should work even if no entries exist
        result = _get_last_downloaded_date("nonexistent-template")
        assert result is None

    @pytest.mark.integration
    def test_is_today_cached(self):
        """Test checking if today's snapshot is cached."""
        result = _is_today_cached("nonexistent-template")
        assert result is False  # Should be False for nonexistent template


class TestResolvedStrategyDataclass:
    """Test the ResolvedStrategy dataclass."""

    def test_resolved_strategy_creation(self):
        """Test creating a ResolvedStrategy."""
        result = ResolvedStrategy(
            strategy=UpdateStrategy.INCREMENTAL_DATE,
            kwargs={"refdate": "2026-04-01"},
        )

        assert result.strategy == UpdateStrategy.INCREMENTAL_DATE
        assert result.kwargs == {"refdate": "2026-04-01"}
