"""Smart update strategy detection and resolution for market data templates.

This module provides convention-based strategy detection and resolution for
incremental updates based on template YAML structure.
"""

from __future__ import annotations

import json
import logging
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING

from brasa.util import DateRange, DownloadArgs

if TYPE_CHECKING:
    from .template import MarketDataTemplate

logger = logging.getLogger(__name__)

DEFAULT_OVERLAP_DAYS = 90
DEFAULT_LOOKBACK_DAYS = 30


class UpdateStrategy(Enum):
    """Update strategy for a template."""

    INCREMENTAL_DATE = "incremental-date"
    INCREMENTAL_DATE_RANGE = "incremental-date-range"
    DAILY_SNAPSHOT = "daily-snapshot"
    DEPENDENCY_DRIVEN = "dependency"
    NO_AUTO_UPDATE = "no-auto-update"


@dataclass
class ResolvedStrategy:
    """Result of resolving an update strategy.

    Contains the kwargs to pass to download_marketdata and force flag.
    """

    strategy: UpdateStrategy
    kwargs: dict
    force: bool = False


def detect_strategy(template: MarketDataTemplate) -> UpdateStrategy:  # noqa: PLR0911
    """Detect update strategy from template YAML structure.

    Convention-based detection order:
    1. If template has `update:` section with `strategy:` field → use that
    2. If template has no downloader (ETL-only) → NO_AUTO_UPDATE
    3. If `start: ~` AND `end: ~` in downloader.args → INCREMENTAL_DATE_RANGE
    4. If `refdate: ~` in downloader.args → INCREMENTAL_DATE
    5. If template has `dependencies:` block → DEPENDENCY_DRIVEN
    6. If template has extra-key: date/datetime (no deps) → DAILY_SNAPSHOT
    7. Otherwise → NO_AUTO_UPDATE

    Args:
        template: The MarketDataTemplate to inspect.

    Returns:
        The detected UpdateStrategy.

    Raises:
        ValueError: If template has unknown strategy in update: section.
    """
    # Check for explicit override in update: section
    if (
        hasattr(template, "update")
        and isinstance(template.update, dict)
        and "strategy" in template.update
    ):
        strategy_str = template.update["strategy"]
        for strategy in UpdateStrategy:
            if strategy.value == strategy_str:
                return strategy
        # Unknown strategy
        raise ValueError(
            f"Unknown update strategy '{strategy_str}' in template {template.id}"
        )

    # ETL-only templates (no downloader)
    if not template.has_downloader:
        return UpdateStrategy.NO_AUTO_UPDATE

    # Check for start: ~ AND end: ~ (INCREMENTAL_DATE_RANGE)
    downloader_args = template.downloader.args
    if (
        "start" in downloader_args
        and "end" in downloader_args
        and downloader_args["start"] is None
        and downloader_args["end"] is None
    ):
        return UpdateStrategy.INCREMENTAL_DATE_RANGE

    # Check for refdate: ~ (INCREMENTAL_DATE)
    if "refdate" in downloader_args and downloader_args["refdate"] is None:
        return UpdateStrategy.INCREMENTAL_DATE

    # Check for dependencies (DEPENDENCY_DRIVEN)
    if hasattr(template, "dependencies") and template.dependencies:
        return UpdateStrategy.DEPENDENCY_DRIVEN

    # Check for extra-key: date/datetime (DAILY_SNAPSHOT) — no deps
    if template.downloader._extra_key in ("date", "datetime") and not (
        hasattr(template, "dependencies") and template.dependencies
    ):
        return UpdateStrategy.DAILY_SNAPSHOT

    # Default: no auto-update
    return UpdateStrategy.NO_AUTO_UPDATE


def resolve_update(
    template_name: str,
    calendar: str = "B3",
    since: str | None = None,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
) -> ResolvedStrategy:
    """Resolve kwargs for an incremental update.

    Based on the detected strategy, query the cache and generate kwargs
    for a smart incremental download.

    Args:
        template_name: Name of the template.
        calendar: Calendar for date operations (default: "B3").
        since: Explicit start date (YYYY-MM-DD). Overrides cache query.
        overlap_days: For INCREMENTAL_DATE_RANGE, days to overlap
            with last download to capture late-published data.

    Returns:
        ResolvedStrategy with kwargs and force flag.

    Raises:
        ValueError: If template not found.
    """
    from .template import retrieve_template

    template = retrieve_template(template_name)
    strategy = detect_strategy(template)

    if strategy == UpdateStrategy.INCREMENTAL_DATE:
        return _resolve_incremental_date(template_name, calendar=calendar, since=since)
    elif strategy == UpdateStrategy.INCREMENTAL_DATE_RANGE:
        return _resolve_incremental_date_range(
            template_name, calendar=calendar, since=since, overlap_days=overlap_days
        )
    elif strategy == UpdateStrategy.DAILY_SNAPSHOT:
        return _resolve_daily_snapshot(template_name)
    elif strategy == UpdateStrategy.DEPENDENCY_DRIVEN:
        return _resolve_dependency_driven()
    else:
        return _resolve_no_auto_update(template_name)


def _resolve_incremental_date(
    template_name: str, calendar: str = "B3", since: str | None = None
) -> ResolvedStrategy:
    """Resolve INCREMENTAL_DATE strategy.

    Last cached date + 1 business day to yesterday.
    """
    last_date = _get_last_downloaded_date(template_name)

    if since:
        # Parse explicit since date
        since_obj = datetime.strptime(since, "%Y-%m-%d").date()
    elif last_date:
        # Use last_date + 1 business day
        since_obj = last_date
    else:
        # 30-day fallback
        since_obj = (datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).date()

    kwargs = {
        "refdate": DateRange(
            start=datetime.combine(since_obj, datetime.min.time()), calendar=calendar
        )
    }
    return ResolvedStrategy(
        strategy=UpdateStrategy.INCREMENTAL_DATE, kwargs=kwargs, force=False
    )


def _resolve_incremental_date_range(
    template_name: str,
    calendar: str = "B3",  # noqa: ARG001
    since: str | None = None,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
) -> ResolvedStrategy:
    """Resolve INCREMENTAL_DATE_RANGE strategy.

    start = last_end - overlap_days
    end = yesterday
    Handles publication lag via overlap buffer.
    """
    last_end = _get_last_downloaded_end_date(template_name)

    if since:
        # Parse explicit since date
        start_obj = datetime.strptime(since, "%Y-%m-%d")
    elif last_end:
        # Use last_end - overlap_days
        start_obj = datetime.combine(
            last_end - timedelta(days=overlap_days), datetime.min.time()
        )
    else:
        # 30-day fallback
        start_obj = datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    end_obj = datetime.now()

    # Check if already up-to-date
    yesterday = datetime.now() - timedelta(days=1)
    if start_obj.date() >= yesterday.date():
        # Nothing to do
        return ResolvedStrategy(
            strategy=UpdateStrategy.INCREMENTAL_DATE_RANGE, kwargs={}, force=False
        )

    kwargs = {
        "start": start_obj.date() if isinstance(start_obj, datetime) else start_obj,
        "end": end_obj.date() if isinstance(end_obj, datetime) else end_obj,
    }
    return ResolvedStrategy(
        strategy=UpdateStrategy.INCREMENTAL_DATE_RANGE, kwargs=kwargs, force=False
    )


def _resolve_daily_snapshot(template_name: str) -> ResolvedStrategy:
    """Resolve DAILY_SNAPSHOT strategy.

    If today is cached, no-op. Otherwise download needed (no kwargs).
    """
    if _is_today_cached(template_name):
        # Already fresh
        return ResolvedStrategy(
            strategy=UpdateStrategy.DAILY_SNAPSHOT, kwargs={}, force=False
        )
    # Download needed
    return ResolvedStrategy(
        strategy=UpdateStrategy.DAILY_SNAPSHOT, kwargs={}, force=False
    )


def _resolve_dependency_driven() -> ResolvedStrategy:
    """Resolve DEPENDENCY_DRIVEN strategy.

    Dependencies are resolved by the existing resolver, so return empty kwargs.
    """
    return ResolvedStrategy(
        strategy=UpdateStrategy.DEPENDENCY_DRIVEN, kwargs={}, force=False
    )


def _resolve_no_auto_update(template_name: str) -> ResolvedStrategy:
    """Resolve NO_AUTO_UPDATE strategy.

    Print warning and return empty result.
    """
    logger.warning(
        f"Template '{template_name}' does not support --update. "
        "Use --force to re-download."
    )
    return ResolvedStrategy(
        strategy=UpdateStrategy.NO_AUTO_UPDATE, kwargs={}, force=False
    )


def get_uncached_kwargs(template_name: str, kwargs: dict) -> tuple[dict, int]:
    """Remove cached arg combos from kwargs.

    Only for templates WITHOUT extra-key. For extra-key templates,
    returns (kwargs, 0) as cache ID reconstruction is impossible.

    Args:
        template_name: Name of the template.
        kwargs: Download kwargs with iterable args (from KwargsIterator).

    Returns:
        Tuple of (filtered_kwargs, skipped_count).
    """
    from brasa.util import KwargsIterator, generate_checksum_for_template

    from .template import retrieve_template

    template = retrieve_template(template_name)

    # Don't filter extra-key templates (cache IDs won't match)
    if template.downloader._extra_key:
        return kwargs, 0

    # Generate all combos from kwargs
    iterator = KwargsIterator(kwargs)
    all_combos = list(iterator)

    if not all_combos:
        return kwargs, 0

    # Check which combos are already cached
    cached_ids = _batch_check_cached(template_name, all_combos)

    # Filter out cached combos
    uncached_combos = [
        combo
        for combo in all_combos
        if generate_checksum_for_template(template_name, DownloadArgs(combo))
        not in cached_ids
    ]

    skipped_count = len(all_combos) - len(uncached_combos)

    # Rebuild kwargs from uncached combos
    if not uncached_combos:
        return {}, skipped_count

    # Reconstruct kwargs from uncached combos
    filtered_kwargs = {}
    for key in kwargs:
        values = {combo[key] for combo in uncached_combos}
        if len(values) == 1:
            filtered_kwargs[key] = next(iter(values))
        else:
            filtered_kwargs[key] = list(values)

    return filtered_kwargs, skipped_count


def _get_last_downloaded_date(template_name: str) -> date | None:
    """Get the max refdate from cache_metadata for a template.

    Deserializes download_args in Python, not SQL.
    Returns None if no entries found.
    """
    from .cache import CacheManager

    cache = CacheManager()
    with closing(cache.meta_db_connection) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT download_args FROM cache_metadata WHERE template = ? "
            "ORDER BY ROWID DESC LIMIT 100",
            (template_name,),
        )
        rows = c.fetchall()

    max_date = None
    for (download_args_json,) in rows:
        try:
            args = DownloadArgs.from_json(download_args_json)
            if "refdate" in args:
                # Extract date from canonical form (YYYY-MM-DDTHH:MM:SS)
                refdate_val = args.get_object("refdate")
                if isinstance(refdate_val, datetime):
                    refdate_date = refdate_val.date()
                    if max_date is None or refdate_date > max_date:
                        max_date = refdate_date
        except (json.JSONDecodeError, ValueError):
            continue

    return max_date


def _get_last_downloaded_end_date(template_name: str) -> date | None:
    """Get the max 'end' date from cache_metadata for a template.

    Uses max() to find the furthest point downloaded so far, so the next
    incremental update continues from there (minus overlap buffer).
    Deserializes download_args in Python, not SQL.
    Returns None if no entries found.
    """
    from .cache import CacheManager

    cache = CacheManager()
    with closing(cache.meta_db_connection) as conn:
        c = conn.cursor()
        # Use SQL MAX on the JSON field to avoid scanning all rows in Python.
        # json_extract returns the value as a string; MAX() gives lexicographic
        # max which works correctly for ISO-8601 datetime strings.
        c.execute(
            "SELECT MAX(json_extract(download_args, '$.end')) "
            "FROM cache_metadata WHERE template = ?",
            (template_name,),
        )
        row = c.fetchone()

    if not row or row[0] is None:
        return None

    try:
        end_val = DownloadArgs.from_json(f'{{"end": "{row[0]}"}}').get_object("end")
        if isinstance(end_val, datetime):
            return end_val.date()
    except (json.JSONDecodeError, ValueError):
        pass

    return None


def _batch_check_cached(template_name: str, combos: list[dict]) -> set[str]:
    """Batch check which arg combos are already cached.

    Uses cache_metadata table with batch queries (chunked at 900).
    Returns set of cache IDs that exist.

    Args:
        template_name: Name of the template.
        combos: List of kwargs dicts (arg combinations).

    Returns:
        Set of cache IDs that are already cached.
    """
    from brasa.util import generate_checksum_for_template

    from .cache import CacheManager

    if not combos:
        return set()

    cache = CacheManager()
    cached_ids = set()

    # Generate cache IDs for all combos
    cache_ids = [
        generate_checksum_for_template(template_name, DownloadArgs(combo))
        for combo in combos
    ]

    # Batch check in chunks of 900 (SQLite limit)
    chunk_size = 900
    with closing(cache.meta_db_connection) as conn:
        c = conn.cursor()
        for i in range(0, len(cache_ids), chunk_size):
            chunk = cache_ids[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            c.execute(
                f"SELECT id FROM cache_metadata WHERE template = ? AND id IN ({placeholders})",
                [template_name, *chunk],
            )
            for (cache_id,) in c.fetchall():
                cached_ids.add(cache_id)

    return cached_ids


def _is_today_cached(template_name: str) -> bool:
    """Check if today's snapshot is cached.

    For DAILY_SNAPSHOT templates, check if there's a cache entry
    with extra_key = today's date.
    """
    from .cache import CacheManager

    today = datetime.now().isoformat()[:10]
    cache = CacheManager()

    with closing(cache.meta_db_connection) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT 1 FROM cache_metadata WHERE template = ? AND extra_key = ?",
            (template_name, today),
        )
        return len(c.fetchall()) > 0
