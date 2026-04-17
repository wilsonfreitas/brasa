"""Tests for extra-key processing semantics.

These tests verify that:
1. Processing two cache entries for the same partition (different extra_key)
   results in only the newest data surviving (delete_matching behaviour).
2. process_marketdata skips stale entries when a newer processed entry
   already exists for the same (template, download_args) group.
"""

import json
from contextlib import closing

import pandas as pd
import pyarrow.dataset as ds

from brasa.engine.cache import CacheManager, CacheMetadata
from brasa.engine.core import json_convert_from_object
from brasa.engine.processing import save_partitioned_parquet_file
from brasa.util import DownloadArgs, generate_checksum_for_template

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_meta(template: str, args: dict, extra_key: str) -> CacheMetadata:
    meta = CacheMetadata(template)
    meta.download_args = DownloadArgs(args)
    meta.extra_key = extra_key
    # Use a unique checksum per entry (download_checksum has a UNIQUE constraint)
    meta.download_checksum = generate_checksum_for_template(
        template, DownloadArgs(args), extra_key
    )
    meta.downloaded_files = []
    return meta


def _save_meta_to_db(cache: CacheManager, meta: CacheMetadata) -> None:
    """Insert a cache entry into the metadata database."""
    download_args = meta.download_args
    with closing(cache.meta_db_connection) as conn, conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO cache_metadata "
            "(id, download_checksum, timestamp, response, download_args, "
            "template, downloaded_files, processed_files, extra_key, "
            "processing_errors, is_invalid_download, invalid_download_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                meta.id,
                meta.download_checksum,
                meta.timestamp.isoformat(),
                json.dumps(meta.response, default=json_convert_from_object),
                download_args.to_json(),
                meta.template,
                json.dumps(meta.downloaded_files, default=json_convert_from_object),
                json.dumps(meta.is_processed),
                meta.extra_key,
                meta.processing_errors,
                "1" if meta.is_invalid_download else "0",
                meta.invalid_download_reason,
            ),
        )


# ---------------------------------------------------------------------------
# Tests: partition "newest wins" via delete_matching
# ---------------------------------------------------------------------------


class TestNewestPartitionWins:
    """Processing two entries for the same partition — newer data survives."""

    def test_second_write_overwrites_partition(self, tmp_path):
        """Processing entry-2 after entry-1 leaves only entry-2 data."""
        partition_folder = str(tmp_path / "data")

        meta1 = _make_meta("tmpl", {"year": "2026", "index": "IBOV"}, "2026-04-15")
        df1 = pd.DataFrame(
            {
                "day": [1, 2],
                "month01": [100.0, 200.0],
                "year": [2026, 2026],
                "index": ["IBOV", "IBOV"],
            }
        )

        meta2 = _make_meta("tmpl", {"year": "2026", "index": "IBOV"}, "2026-04-16")
        df2 = pd.DataFrame(
            {
                "day": [1, 2],
                "month01": [110.0, 220.0],
                "year": [2026, 2026],
                "index": ["IBOV", "IBOV"],
            }
        )

        save_partitioned_parquet_file(meta1, partition_folder, df1, ["index", "year"])
        save_partitioned_parquet_file(meta2, partition_folder, df2, ["index", "year"])

        result = (
            ds.dataset(partition_folder, format="parquet", partitioning="hive")
            .to_table()
            .to_pandas()
        )
        # Partition only contains the data from the second (newer) write
        assert set(result["month01"].tolist()) == {110.0, 220.0}
        assert len(result) == 2

    def test_first_write_also_overwrites(self, tmp_path):
        """Processing entry-1 after entry-2 (reverse order) shows entry-1 wins.

        This is the undesirable case that the processing safeguard prevents.
        With no safeguard and reprocess=True, the last writer wins regardless
        of chronological order.
        """
        partition_folder = str(tmp_path / "data")

        meta1 = _make_meta("tmpl", {"year": "2026", "index": "IBOV"}, "2026-04-15")
        df1 = pd.DataFrame(
            {
                "day": [1, 2],
                "month01": [100.0, 200.0],
                "year": [2026, 2026],
                "index": ["IBOV", "IBOV"],
            }
        )

        meta2 = _make_meta("tmpl", {"year": "2026", "index": "IBOV"}, "2026-04-16")
        df2 = pd.DataFrame(
            {
                "day": [1, 2],
                "month01": [110.0, 220.0],
                "year": [2026, 2026],
                "index": ["IBOV", "IBOV"],
            }
        )

        # Write newer first, then older — stale data wins (reverse order)
        save_partitioned_parquet_file(meta2, partition_folder, df2, ["index", "year"])
        save_partitioned_parquet_file(meta1, partition_folder, df1, ["index", "year"])

        result = (
            ds.dataset(partition_folder, format="parquet", partitioning="hive")
            .to_table()
            .to_pandas()
        )
        # Without safeguard, older data stomped the newer write
        assert set(result["month01"].tolist()) == {100.0, 200.0}


# ---------------------------------------------------------------------------
# Tests: _get_stale_extra_key_ids safeguard
# ---------------------------------------------------------------------------


class TestGetStaleExtraKeyIds:
    """Unit tests for the stale-entry filter."""

    def test_no_entries_returns_empty(self):
        """No cache entries → no stale IDs."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()
        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        assert result == set()

    def test_single_entry_not_stale(self):
        """A single entry, even if processed, is not stale (nothing newer)."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()
        meta = _make_meta(
            "b3-indexes-historical-prices",
            {"year": "2026", "index": "IBOV", "language": "pt-br"},
            "2026-04-15",
        )
        meta.mark_as_processed()
        _save_meta_to_db(cache, meta)

        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        assert result == set()

    def test_older_entry_stale_when_newer_processed(self):
        """Older entry is stale if a newer processed entry exists for same args."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()
        args = {"year": "2026", "index": "IBOV", "language": "pt-br"}

        meta_old = _make_meta("b3-indexes-historical-prices", args, "2026-04-15")
        meta_new = _make_meta("b3-indexes-historical-prices", args, "2026-04-16")
        meta_new.mark_as_processed()

        _save_meta_to_db(cache, meta_old)
        _save_meta_to_db(cache, meta_new)

        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        assert meta_old.id in result
        assert meta_new.id not in result

    def test_older_entry_not_stale_when_newer_unprocessed(self):
        """Older entry is NOT stale if the newer entry hasn't been processed yet."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()
        args = {"year": "2026", "index": "IBOV", "language": "pt-br"}

        meta_old = _make_meta("b3-indexes-historical-prices", args, "2026-04-15")
        meta_new = _make_meta("b3-indexes-historical-prices", args, "2026-04-16")
        # meta_new is NOT processed

        _save_meta_to_db(cache, meta_old)
        _save_meta_to_db(cache, meta_new)

        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        assert result == set()

    def test_empty_extra_key_entries_ignored(self):
        """Entries with empty extra_key (pre-migration) are never filtered."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()
        args = {"year": "2025", "index": "IBOV", "language": "pt-br"}

        # Old-style entry (no extra_key)
        meta_old = _make_meta("b3-indexes-historical-prices", args, "")
        # New-style entry for the same args
        meta_new = _make_meta("b3-indexes-historical-prices", args, "2026-04-16")
        meta_new.mark_as_processed()

        _save_meta_to_db(cache, meta_old)
        _save_meta_to_db(cache, meta_new)

        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        # Empty-extra_key entry excluded from stale check
        assert meta_old.id not in result

    def test_different_download_args_independent(self):
        """Staleness check is per download_args — different args never interact."""
        from brasa.engine.api import _get_stale_extra_key_ids

        cache = CacheManager()

        # IBOV: day1 old (unprocessed), day2 new (processed)
        ibov_old = _make_meta(
            "b3-indexes-historical-prices",
            {"year": "2026", "index": "IBOV", "language": "pt-br"},
            "2026-04-15",
        )
        ibov_new = _make_meta(
            "b3-indexes-historical-prices",
            {"year": "2026", "index": "IBOV", "language": "pt-br"},
            "2026-04-16",
        )
        ibov_new.mark_as_processed()

        # IFIX: single entry (no staleness)
        ifix = _make_meta(
            "b3-indexes-historical-prices",
            {"year": "2026", "index": "IFIX", "language": "pt-br"},
            "2026-04-15",
        )

        for m in (ibov_old, ibov_new, ifix):
            _save_meta_to_db(cache, m)

        result = _get_stale_extra_key_ids("b3-indexes-historical-prices", cache)
        assert ibov_old.id in result
        assert ibov_new.id not in result
        assert ifix.id not in result
