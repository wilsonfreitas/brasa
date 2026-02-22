"""Cache management for market data.

This module provides the CacheManager singleton and CacheMetadata class
for managing downloaded and processed market data files, including
SQLite-based metadata persistence.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from warnings import warn

import pandas as pd

from brasa.util import generate_checksum_for_template

from .core import Singleton, json_convert_from_object, json_convert_to_object

if TYPE_CHECKING:
    from .template import MarketDataTemplate


@dataclass(frozen=True)
class DownloadResult:
    """Result of a download attempt from CacheManager.download_marketdata.

    Encapsulates the full outcome so callers never need to catch
    download-related exceptions themselves.

    Attributes:
        status_code: Single-char status symbol ('.', 'D', 'I', 'F', 'E').
        status_name: Human-readable name (e.g. 'PASSED', 'FAILED').
        reason: Descriptive text (usually str(exception) or empty).
        http_status: HTTP status code when available, else None.
        is_success: True for PASSED/DUPLICATED outcomes.
        is_expected_error: True for expected failures (I, F), False for E.
        exception: The original exception object, or None on success.
        retry_attempts_used: Number of retry attempts actually executed
            (0 means first attempt succeeded). None when retry is not
            configured.
        retry_attempts_configured: max extra attempts configured via
            template retry_attempts. None when not configured.
        retry_success_on_attempt: 1-based attempt number that succeeded,
            or None if all attempts failed.
    """

    status_code: str
    status_name: str
    reason: str = ""
    http_status: int | None = None
    is_success: bool = True
    is_expected_error: bool = False
    exception: Exception | None = None
    retry_attempts_used: int | None = None
    retry_attempts_configured: int | None = None
    retry_success_on_attempt: int | None = None


def _extract_http_status(ex: Exception) -> int | None:
    """Extract HTTP status code from a DownloadException message.

    Parses patterns like ``status_code = 404`` from the exception
    message string.

    Args:
        ex: The exception to inspect.

    Returns:
        The HTTP status code as integer, or None if not found.
    """
    match = re.search(r"status_code\s*=\s*(\d+)", str(ex))
    if match:
        return int(match.group(1))
    return None


class CacheMetadata:
    """Metadata for a cached market data download.

    Tracks download arguments, checksums, file paths, and processing state.
    """

    def __init__(self, template: str) -> None:
        self.template: str = template
        self.timestamp: datetime = datetime.now()
        self.response: Any = None
        self.download_checksum: str = ""
        self.download_args: dict[str, Any] = {}
        self._downloaded_files: list[str] = []
        self._processed_files: dict[str, str] = {}
        self.extra_key: str = ""
        self.processing_errors: str = ""
        self.is_invalid_download: bool = False
        self.invalid_download_reason: str = ""

    def to_dict(self) -> dict:
        """Convert metadata to a dictionary."""
        return {
            "download_checksum": self.download_checksum,
            "timestamp": self.timestamp,
            "response": self.response,
            "download_args": self.download_args,
            "template": self.template,
            "downloaded_files": self.downloaded_files,
            "processed_files": self.processed_files,
            "extra_key": self.extra_key,
            "processing_errors": self.processing_errors,
            "is_invalid_download": self.is_invalid_download,
            "invalid_download_reason": self.invalid_download_reason,
        }

    def from_dict(self, kwargs) -> None:
        """Load metadata from a dictionary."""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def normalize_path(self, path: str) -> str:
        """Normalize path separators for cross-platform compatibility."""
        normalized = path.replace("\\", "/")
        return str(Path(normalized))

    @property
    def downloaded_files(self) -> list[str]:
        """List of downloaded file paths (normalized)."""
        return [self.normalize_path(f) for f in self._downloaded_files]

    @downloaded_files.setter
    def downloaded_files(self, value: list[str]) -> None:
        self._downloaded_files = value

    @property
    def processed_files(self) -> dict[str, str]:
        """Dictionary of processed file names to paths."""
        return self._processed_files

    @processed_files.setter
    def processed_files(self, value: dict[str, str]) -> None:
        self._processed_files = value

    def add_processed_file(self, key: str, path: str) -> None:
        """Add a processed file."""
        self._processed_files[key] = path

    def add_downloaded_file(self, path: str) -> None:
        """Add a downloaded file."""
        self._downloaded_files.append(path)

    def remove_downloaded_file(self, path: str) -> None:
        """Remove a downloaded file."""
        if path in self._downloaded_files:
            self._downloaded_files.remove(path)

    def remove_processed_file(self, key: str) -> None:
        """Remove a processed file."""
        self._processed_files.pop(key, None)

    @property
    def id(self) -> str:
        """Unique identifier for this cache entry."""
        return generate_checksum_for_template(
            self.template, self.download_args, self.extra_key
        )

    @property
    def download_folder(self) -> str:
        """Path to the download folder for this cache entry."""
        if self.download_checksum == "":
            return ""
        else:
            return str(
                Path(CacheManager._raw_folder) / self.template / self.download_checksum
            )


class CacheManager(Singleton):
    """Singleton manager for the market data cache.

    Handles file system structure, SQLite metadata storage, and
    coordination of download/processing operations.
    """

    _meta_db_filename = "meta.db"
    _meta_folder = "meta"
    _duckdb_filename = "brasa.duckdb"
    _db_folder = "db"
    _raw_folder = "raw"

    def init(self) -> None:
        """Initialize the cache manager and create necessary directories."""
        self._cache_folder = os.environ.get(
            "BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache")
        )
        Path(self._cache_folder).mkdir(parents=True, exist_ok=True)
        Path(self.cache_path(self._meta_folder)).mkdir(parents=True, exist_ok=True)
        Path(self.cache_path(self._db_folder)).mkdir(parents=True, exist_ok=True)
        if not Path(self.cache_path(self.meta_db_filename)).exists():
            self.create_meta_db()
        else:
            self._migrate_download_trials()
        # Initialize the dataset catalog table
        self._init_dataset_catalog()

    @property
    def cache_folder(self) -> str:
        """Root folder for the cache."""
        return self._cache_folder

    def cache_path(self, fname: str) -> str:
        """Get the full path for a file within the cache."""
        if "\\" in fname:
            parts = fname.split("\\")
            path = Path(self.cache_folder, *parts)
        else:
            path = Path(self.cache_folder, fname)
        return str(path)

    def db_path(self, name: str) -> str:
        """Get the path for a database file.

        Args:
            name: The relative path within the db folder. Can include
                layer prefix (e.g., 'input/template-id').

        Returns:
            Absolute path to the database file or folder.
        """
        return str(Path(self.cache_path(self._db_folder)) / name)

    @property
    def duckdb_filename(self) -> str:
        """Path to the DuckDB database file."""
        return str(Path(self._db_folder) / self._duckdb_filename)

    def create_meta_db(self) -> None:
        """Create the SQLite metadata database."""
        db_conn = sqlite3.connect(database=self.cache_path(self.meta_db_filename))
        c = db_conn.cursor()
        sql_path = Path(__file__).parent / ".." / ".." / "sql" / "create-meta-db.sql"
        with sql_path.open() as f:
            c.executescript(f.read())
        db_conn.commit()
        db_conn.close()

    def _init_dataset_catalog(self) -> None:
        """Initialize the dataset catalog table if it doesn't exist."""
        db_conn = sqlite3.connect(database=self.cache_path(self.meta_db_filename))
        c = db_conn.cursor()
        sql_path = (
            Path(__file__).parent / ".." / ".." / "sql" / "create-dataset-catalog.sql"
        )
        with sql_path.open() as f:
            c.executescript(f.read())
        db_conn.commit()
        db_conn.close()

    def _migrate_download_trials(self) -> None:
        """Add missing status columns to download_trials (idempotent).

        Checks existing columns via PRAGMA table_info and adds any
        missing columns. Also backfills legacy rows where status_code
        is NULL: downloaded=1 -> PASSED, downloaded=0 -> FAILED.
        """
        new_columns = {
            "status_code": "TEXT",
            "status_name": "TEXT",
            "reason": "TEXT",
            "http_status": "INTEGER",
        }
        db_conn = sqlite3.connect(database=self.cache_path(self.meta_db_filename))
        c = db_conn.cursor()

        c.execute("PRAGMA table_info(download_trials)")
        existing = {row[1] for row in c.fetchall()}

        for col_name, col_type in new_columns.items():
            if col_name not in existing:
                c.execute(
                    f"ALTER TABLE download_trials ADD COLUMN {col_name} {col_type}"
                )

        # Backfill legacy rows that have no status_code
        c.execute(
            "UPDATE download_trials SET status_code = '.', status_name = 'PASSED' "
            "WHERE status_code IS NULL AND downloaded = '1'"
        )
        c.execute(
            "UPDATE download_trials SET status_code = 'F', status_name = 'FAILED' "
            "WHERE status_code IS NULL AND downloaded = '0'"
        )

        db_conn.commit()
        db_conn.close()

    @property
    def meta_db_connection(self) -> sqlite3.Connection:
        """Get a connection to the metadata database."""
        return sqlite3.connect(database=self.cache_path(self.meta_db_filename))

    def db_folder(self, template: MarketDataTemplate) -> str:
        """Get the database folder for a template, including layer.

        The folder path includes the data layer from the template's writer
        configuration. Structure: db/{layer}/{dataset-name}

        Uses writer.dataset if specified, otherwise falls back to template.id.

        Args:
            template: The template to get the folder for.

        Returns:
            Relative path to the database folder within the cache.
        """
        layer = template.writer.layer.value
        dataset_name = template.writer.dataset
        folder = str(Path(self._db_folder) / layer / dataset_name)
        Path(self.cache_path(folder)).mkdir(parents=True, exist_ok=True)
        return folder

    def db_folders(self, template: MarketDataTemplate) -> dict:
        """Get database folders for a multi-output template.

        Supports both new datasets attribute and legacy reader.multi.
        For datasets: uses output names directly as folder suffixes.
        For legacy multi: uses the mapped output names as folder suffixes.

        The folder paths include the data layer from the template's writer
        configuration. Structure: db/{layer}/{dataset-name}-{output-name}

        Uses writer.dataset if specified, otherwise falls back to template.id.

        Args:
            template: The template to get folders for.

        Returns:
            Dictionary mapping output names to folder paths.
        """
        db_folders = {}
        layer = template.writer.layer.value
        dataset_name = template.writer.dataset

        if template.datasets:
            # New approach: use datasets output names directly
            for output_name in template.datasets:
                folder = str(
                    Path(self._db_folder) / layer / f"{dataset_name}-{output_name}"
                )
                Path(self.cache_path(folder)).mkdir(parents=True, exist_ok=True)
                db_folders[output_name] = folder
        elif template.reader.multi:
            # Legacy fallback: use multi mapping (XML tag -> output name)
            for name, val in template.reader.multi.items():
                folder = str(Path(self._db_folder) / layer / f"{dataset_name}-{val}")
                Path(self.cache_path(folder)).mkdir(parents=True, exist_ok=True)
                db_folders[name] = folder

        return db_folders

    def create_download_folder(self, meta: CacheMetadata):
        """Create the download folder for a cache entry."""
        Path(self.cache_path(meta.download_folder)).mkdir(parents=True, exist_ok=True)

    @property
    def meta_db_filename(self) -> str:
        """Relative path to the metadata database."""
        return str(Path(self._meta_folder) / self._meta_db_filename)

    @property
    def meta_folder(self) -> str:
        """Metadata folder path."""
        Path(self.cache_path(self._meta_folder)).mkdir(parents=True, exist_ok=True)
        return self._meta_folder

    def meta_file_path(self, meta: CacheMetadata) -> str:
        """Get the file path for a metadata entry."""
        return str(Path(self.cache_folder) / self.meta_folder / f"{meta.id}.yaml")

    def has_meta(self, meta: CacheMetadata) -> bool:
        """Check if metadata exists for a cache entry."""
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (meta.id,))
            return len(c.fetchall()) == 1

    def load_meta(self, meta: CacheMetadata) -> None:
        """Load metadata from the database into a CacheMetadata instance."""
        _meta = self._load_meta_dict_by_id(meta.id)
        meta.from_dict(_meta)

    def _load_meta_dict_by_id(self, id: str) -> dict | None:
        """Load metadata as a dictionary by ID."""
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (id,))
            if meta_row := c.fetchall():
                meta_row = meta_row[0]
                _meta = {
                    "download_checksum": meta_row[1],
                    "timestamp": datetime.fromisoformat(meta_row[2]),
                    "response": json.loads(
                        meta_row[3], object_hook=json_convert_to_object
                    ),
                    "download_args": json.loads(
                        meta_row[4], object_hook=json_convert_to_object
                    ),
                    "template": meta_row[5],
                    "downloaded_files": json.loads(
                        meta_row[6], object_hook=json_convert_to_object
                    ),
                    "processed_files": json.loads(
                        meta_row[7], object_hook=json_convert_to_object
                    ),
                    "extra_key": meta_row[8],
                    "processing_errors": meta_row[9],
                    "is_invalid_download": meta_row[10] == "1"
                    if len(meta_row) > 10
                    else False,
                    "invalid_download_reason": meta_row[11]
                    if len(meta_row) > 11
                    else "",
                }
                return _meta
        return None

    def save_meta(self, meta: CacheMetadata) -> None:
        """Save metadata to the database."""
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (meta.id,))
            if c.fetchall():
                params = (
                    meta.download_checksum,
                    meta.timestamp.isoformat(),
                    json.dumps(meta.response, default=json_convert_from_object),
                    json.dumps(meta.download_args, default=json_convert_from_object),
                    meta.template,
                    json.dumps(meta.downloaded_files, default=json_convert_from_object),
                    json.dumps(meta.processed_files, default=json_convert_from_object),
                    meta.extra_key,
                    meta.processing_errors,
                    "1" if meta.is_invalid_download else "0",
                    meta.invalid_download_reason,
                    meta.id,
                )
                c.execute(
                    "update cache_metadata set download_checksum = ?, timestamp = ?, response = ?, download_args = ?, template = ?, downloaded_files = ?, processed_files = ?, extra_key = ?, processing_errors = ?, is_invalid_download = ?, invalid_download_reason = ? where id = ?",
                    params,
                )
            else:
                params = (
                    meta.id,
                    meta.download_checksum,
                    meta.timestamp.isoformat(),
                    json.dumps(meta.response, default=json_convert_from_object),
                    json.dumps(meta.download_args, default=json_convert_from_object),
                    meta.template,
                    json.dumps(meta.downloaded_files, default=json_convert_from_object),
                    json.dumps(meta.processed_files, default=json_convert_from_object),
                    meta.extra_key,
                    meta.processing_errors,
                    "1" if meta.is_invalid_download else "0",
                    meta.invalid_download_reason,
                )
                c.execute(
                    "insert into cache_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params,
                )
            conn.commit()

    def clean_meta_raw_folder(self, meta: CacheMetadata) -> None:
        """Clean the raw download folder for a cache entry."""
        # warn(f"Cleaning meta download {meta.download_args}", stacklevel=2)
        if meta.download_folder == "":
            return
        folder = self.cache_path(meta.download_folder)
        if Path(folder).exists():
            shutil.rmtree(folder, ignore_errors=False)

    def clean_meta_db_folder(self, meta: CacheMetadata) -> None:
        """Clean the database folder for a cache entry."""
        # warn(f"Cleaning meta db {meta.download_args}", stacklevel=2)
        for processed_fname in meta.processed_files.values():
            full_path = Path(self.cache_path(processed_fname))
            if full_path.is_file():
                full_path.unlink()

    def clean_meta_db(self, meta: CacheMetadata) -> None:
        """Remove metadata from the database."""
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("delete from cache_metadata where id = ?", (meta.id,))
            conn.commit()

    def remove_meta(self, meta: CacheMetadata) -> None:
        """Remove all traces of a cache entry (files and metadata)."""
        self.clean_meta_raw_folder(meta)
        self.clean_meta_db_folder(meta)
        self.clean_meta_db(meta)

    def save_trial(
        self,
        meta: CacheMetadata,
        downloaded: bool,
        status_code: str | None = None,
        status_name: str | None = None,
        reason: str = "",
        http_status: int | None = None,
    ) -> None:
        """Record a download trial with explicit status.

        When status_code/status_name are omitted the method falls back
        to legacy boolean semantics for backward compatibility:
        downloaded=True  -> PASSED (.)
        downloaded=False -> FAILED (F)

        Args:
            meta: Cache metadata identifying the trial.
            downloaded: Legacy boolean success flag.
            status_code: Single-char status symbol (e.g. '.', 'F', 'D').
            status_name: Human-readable status name (e.g. 'PASSED').
            reason: Optional reason text for the outcome.
            http_status: Optional HTTP status code from downloader.
        """
        if status_code is None:
            status_code = "." if downloaded else "F"
            status_name = "PASSED" if downloaded else "FAILED"

        with self.meta_db_connection as conn:
            c = conn.cursor()
            params = (
                meta.id,
                meta.timestamp.isoformat(),
                downloaded,
                status_code,
                status_name,
                reason,
                http_status,
            )
            c.execute(
                "INSERT INTO download_trials "
                "(cache_id, timestamp, downloaded, status_code, status_name, reason, http_status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                params,
            )
        conn.commit()

    def get_last_download_status(self, meta: CacheMetadata) -> dict | None:
        """Get the most recent download status for a cache entry.

        Uses rowid ordering to ensure the latest persisted attempt is
        returned even when multiple trials share the same timestamp
        (e.g. during retry sequences).

        Args:
            meta: Cache metadata identifying the entry.

        Returns:
            Dict with keys code, name, reason, http_status or None
            if no trials exist.
        """
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute(
                "SELECT status_code, status_name, reason, http_status "
                "FROM download_trials "
                "WHERE cache_id = ? ORDER BY rowid DESC LIMIT 1",
                (meta.id,),
            )
            row = c.fetchone()
            if row:
                return {
                    "code": row[0],
                    "name": row[1],
                    "reason": row[2] or "",
                    "http_status": row[3],
                }
        return None

    def has_successful_trial(self, meta: CacheMetadata) -> bool:
        """Check if there was a successful download trial."""
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute(
                "select * from download_trials where cache_id = ? and downloaded == 1",
                (meta.id,),
            )
            return len(c.fetchall()) > 0

    def count_trials(
        self,
        meta: CacheMetadata,
        since: str | None = None,
    ) -> int:
        """Count download trial rows for a cache entry.

        Args:
            meta: Cache metadata identifying the entry.
            since: Optional ISO-format timestamp lower bound
                (inclusive). When provided only trials with
                ``timestamp >= since`` are counted.

        Returns:
            Number of matching trial rows.
        """
        with self.meta_db_connection as conn:
            c = conn.cursor()
            if since:
                c.execute(
                    "SELECT COUNT(*) FROM download_trials "
                    "WHERE cache_id = ? AND timestamp >= ?",
                    (meta.id, since),
                )
            else:
                c.execute(
                    "SELECT COUNT(*) FROM download_trials " "WHERE cache_id = ?",
                    (meta.id,),
                )
            row = c.fetchone()
            return row[0] if row else 0

    def parquet_file_name(self, fname_part: str) -> str:
        """Generate a parquet filename from a part identifier."""
        if re.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", fname_part):
            fname = f"{fname_part}.parquet"
        else:
            fname = f"part-{fname_part}.parquet"
        return fname

    def read_marketdata(self, meta: CacheMetadata):
        """Read and process market data for a cache entry."""
        from .processing import _read_marketdata

        _read_marketdata(meta)
        self.save_meta(meta)

    def load_marketdata(
        self, meta: CacheMetadata, reprocess: bool = False
    ) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
        """Load processed market data from cache."""
        if reprocess:
            self.read_marketdata(meta)
        if len(meta.processed_files) > 0:
            dfs = {
                key: pd.read_parquet(self.cache_path(fname))
                for key, fname in meta.processed_files.items()
            }
            if len(dfs) == 1:
                return dfs["data"]
            else:
                return dfs
        else:
            warn("No processed files", stacklevel=2)
            return None

    def download_marketdata(self, meta: CacheMetadata) -> DownloadResult:
        """Download market data and save to cache.

        Handles all download exceptions internally — callers never need
        to catch download-related errors.  Every outcome is persisted as
        a trial and returned as a ``DownloadResult``.

        When retry is configured in the template, intermediate failed
        attempts are persisted as individual ``download_trials`` rows
        (RSTS-005). The final outcome is always the authoritative
        row for scheduling/reporting.

        Returns:
            A DownloadResult describing the outcome.
        """
        from .download import _download_marketdata
        from .exceptions import (
            CorruptedContentException,
            DownloadException,
            DuplicatedFolderException,
            InvalidContentException,
        )

        # Build per-attempt callback for intermediate retry failures
        def _on_attempt_failure(
            attempt: int, err: Exception, status_code: int | None
        ) -> None:
            http_st = status_code
            if http_st is None:
                http_st = _extract_http_status(err)
            self.save_trial(
                meta,
                downloaded=False,
                status_code="F",
                status_name="FAILED",
                reason=f"retry attempt {attempt}: {err}",
                http_status=http_st,
            )

        result: DownloadResult | None = None
        retry_info: dict = {}
        try:
            retry_info = _download_marketdata(
                meta,
                on_attempt_failure=_on_attempt_failure,
                **meta.download_args,
            )
            self.save_trial(
                meta,
                downloaded=True,
                status_code=".",
                status_name="PASSED",
            )
            self.save_meta(meta)
            result = DownloadResult(
                status_code=".",
                status_name="PASSED",
                retry_attempts_used=retry_info.get("attempts_used"),
                retry_attempts_configured=retry_info.get("attempts_configured"),
                retry_success_on_attempt=retry_info.get("success_on_attempt"),
            )
        except DuplicatedFolderException as e:
            self.save_trial(
                meta,
                downloaded=True,
                status_code="D",
                status_name="DUPLICATED",
                reason=str(e),
            )
            self.clean_meta_db(meta)
            result = DownloadResult(
                status_code="D",
                status_name="DUPLICATED",
                reason=str(e),
                exception=e,
            )
        except InvalidContentException as e:
            self.save_trial(
                meta,
                downloaded=False,
                status_code="I",
                status_name="INVALID",
                reason=str(e),
            )
            self.clean_meta_db(meta)
            self.clean_meta_raw_folder(meta)
            result = DownloadResult(
                status_code="I",
                status_name="INVALID",
                reason=str(e),
                is_success=False,
                is_expected_error=True,
                exception=e,
            )
        except CorruptedContentException as e:
            self.save_trial(
                meta,
                downloaded=False,
                status_code="C",
                status_name="CORRUPTED",
                reason=str(e),
            )
            self.clean_meta_db(meta)
            self.clean_meta_raw_folder(meta)
            result = DownloadResult(
                status_code="C",
                status_name="CORRUPTED",
                reason=str(e),
                is_success=False,
                is_expected_error=True,
                exception=e,
            )
        except DownloadException as e:
            http_status = _extract_http_status(e)
            self.save_trial(
                meta,
                downloaded=False,
                status_code="F",
                status_name="FAILED",
                reason=str(e),
                http_status=http_status,
            )
            result = DownloadResult(
                status_code="F",
                status_name="FAILED",
                reason=str(e),
                http_status=http_status,
                is_success=False,
                is_expected_error=True,
                exception=e,
            )
        except Exception as e:
            self.save_trial(
                meta,
                downloaded=False,
                status_code="E",
                status_name="ERROR",
                reason=str(e),
            )
            self.clean_meta_db(meta)
            self.clean_meta_raw_folder(meta)
            result = DownloadResult(
                status_code="E",
                status_name="ERROR",
                reason=str(e),
                is_success=False,
                is_expected_error=False,
                exception=e,
            )
        return result  # type: ignore[return-value]

    def process_without_checks(
        self, meta: CacheMetadata
    ) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
        """Download and process market data without cache checks."""
        from .download import _download_marketdata

        _download_marketdata(meta, **meta.download_args)
        self.save_meta(meta)
        if len(meta.downloaded_files) > 0:
            return self.load_marketdata(meta, reprocess=True)
        else:
            warn("No downloaded files", stacklevel=2)
            return None
