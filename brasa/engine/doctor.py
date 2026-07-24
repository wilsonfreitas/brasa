"""Diagnostic checks for the brasa cache and template consistency.

This module implements the ``brasa doctor`` command, which surfaces
issues in the local cache: orphan files, broken metadata references,
corrupted parquet files, schema drift, stale ETL outputs, and date gaps.

Usage::

    from brasa.engine.doctor import run_doctor, DoctorReport

    report = run_doctor()
    print(report.summary())
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from collections.abc import Callable
from contextlib import closing, suppress
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class Issue:
    """A single diagnostic issue found during doctor checks.

    Attributes:
        category: High-level category (e.g. "Raw Files", "DB / Parquet").
        code: Short kebab-case code (e.g. "orphan-raw").
        severity: One of "error", "warning", or "info".
        description: Human-readable summary of the issue.
        details: List of affected paths or identifiers.
        fixable: Whether ``--fix`` can resolve this automatically.
        fix_fn: Callable that applies the fix (called only when ``--fix``
            is active and the user confirms).
    """

    category: str
    code: str
    severity: str  # "error" | "warning" | "info"
    description: str
    details: list[str] = field(default_factory=list)
    fixable: bool = False
    fix_fn: Callable[[], None] | None = None


@dataclass
class DoctorReport:
    """Aggregated result of all doctor checks.

    Attributes:
        issues: All issues found across every check.
    """

    issues: list[Issue] = field(default_factory=list)

    def errors(self) -> list[Issue]:
        """Return issues with severity 'error'."""
        return [i for i in self.issues if i.severity == "error"]

    def warnings(self) -> list[Issue]:
        """Return issues with severity 'warning'."""
        return [i for i in self.issues if i.severity == "warning"]

    def infos(self) -> list[Issue]:
        """Return issues with severity 'info'."""
        return [i for i in self.issues if i.severity == "info"]

    def fixable(self) -> list[Issue]:
        """Return issues that can be auto-fixed."""
        return [i for i in self.issues if i.fixable]

    def summary(self) -> str:
        """Return a one-line summary string."""
        n_errors = len(self.errors())
        n_warnings = len(self.warnings())
        n_fixable = len(self.fixable())
        parts = []
        if n_errors:
            parts.append(f"{n_errors} error{'s' if n_errors != 1 else ''}")
        if n_warnings:
            parts.append(f"{n_warnings} warning{'s' if n_warnings != 1 else ''}")
        if not parts:
            parts.append("no issues")
        summary = " · ".join(parts)
        if n_fixable:
            summary += f" · {n_fixable} fixable"
        return summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_meta_connection() -> sqlite3.Connection:
    """Return a new SQLite connection to the metadata database."""
    from .cache import CacheManager

    man = CacheManager()
    return sqlite3.connect(database=man.cache_path(man.meta_db_filename))


def _parse_is_processed(raw: str | None) -> bool:
    """Parse the processed_files column as a boolean (handles migration).

    Args:
        raw: Raw JSON string from the processed_files column.

    Returns:
        True if processed, False otherwise.
    """
    if not raw:
        return False
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return False
    if isinstance(parsed, bool):
        return parsed
    if isinstance(parsed, dict):
        return bool(parsed)  # migration: non-empty dict → True
    return bool(parsed)


def _iter_all_metadata_rows() -> list[dict[str, Any]]:
    """Return all rows from ``cache_metadata`` as dicts."""
    with closing(_get_meta_connection()) as conn, conn:
        c = conn.cursor()
        c.execute(
            "SELECT id, download_checksum, template, downloaded_files, "
            "processed_files, processing_errors, is_invalid_download "
            "FROM cache_metadata"
        )
        rows = []
        for row in c.fetchall():
            rows.append(
                {
                    "id": row[0],
                    "download_checksum": row[1],
                    "template": row[2],
                    "downloaded_files": json.loads(row[3] or "[]"),
                    "is_processed": _parse_is_processed(row[4]),
                    "processing_errors": row[5] or "",
                    "is_invalid_download": row[6] == "1",
                }
            )
        return rows


def _delete_meta_row(meta_id: str) -> None:
    """Delete a single row from ``cache_metadata`` by id."""
    with closing(_get_meta_connection()) as conn, conn:
        c = conn.cursor()
        c.execute("DELETE FROM cache_metadata WHERE id = ?", (meta_id,))


def _mark_meta_invalid(meta_id: str) -> None:
    """Mark a ``cache_metadata`` row as invalid."""
    with closing(_get_meta_connection()) as conn, conn:
        c = conn.cursor()
        c.execute(
            "UPDATE cache_metadata SET is_invalid_download = '1', "
            "invalid_download_reason = 'doctor: missing raw files' "
            "WHERE id = ?",
            (meta_id,),
        )


def _get_catalog_rows() -> list[dict[str, Any]]:
    """Return all rows from ``dataset_catalog`` as dicts."""
    with closing(_get_meta_connection()) as conn, conn:
        c = conn.cursor()
        try:
            c.execute(
                "SELECT id, layer, dataset_name, schema_json, "
                "source_template, updated_at FROM dataset_catalog"
            )
            rows = []
            for row in c.fetchall():
                rows.append(
                    {
                        "id": row[0],
                        "layer": row[1],
                        "dataset_name": row[2],
                        "schema_json": row[3],
                        "source_template": row[4],
                        "updated_at": row[5],
                    }
                )
            return rows
        except sqlite3.OperationalError:
            return []


def _load_validations_config(path: Path) -> dict[str, Any]:
    """Load the validations config mapping (layer.dataset -> list of rules).

    Args:
        path: Path to a validations YAML file. Required — there is no
            packaged default.

    Returns:
        Parsed mapping of ``"layer.dataset"`` to a list of rules.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If the file is empty or its YAML root is not a mapping.
        yaml.YAMLError: If the file cannot be parsed.
    """
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"validations file not found: {cfg_path}")
    with cfg_path.open() as f:
        data = yaml.safe_load(f)
    if data is None:
        raise ValueError(f"validations file is empty: {cfg_path}")
    if not isinstance(data, dict):
        raise ValueError(
            "validations config root must be a mapping of "
            "'layer.dataset' -> list of rules"
        )
    return data


def _resolve_series_dataset(
    dataset_key: str,
    dataset_path: Path,
    date_column: str,
    group_column: str | None,
) -> Any | None:
    """Open ``dataset_path`` typed via the template/catalog schema when possible.

    Falls back to naive Hive partition discovery (partition columns typed as
    strings) when there's no template/catalog schema covering the columns
    this rule needs, e.g. ad hoc datasets with no registered template.
    """
    import pyarrow.dataset as ds

    from brasa.queries import get_dataset

    layer, _, name = dataset_key.partition(".")
    try:
        dataset = get_dataset(name, layer=layer)
    except Exception:
        dataset = None

    columns_covered = dataset is not None and date_column in dataset.schema.names
    if columns_covered and group_column is not None:
        columns_covered = group_column in dataset.schema.names
    if columns_covered:
        return dataset

    try:
        return ds.dataset(str(dataset_path), partitioning="hive")
    except Exception:
        return None


def _read_series_dates(
    dataset_key: str,
    dataset_path: Path,
    date_column: str,
    group_column: str | None,
    group_value: str | None,
) -> set[date]:
    """Return the distinct dates present for one series.

    Args:
        dataset_key: ``"layer.dataset"`` key (e.g. ``"input.b3-bvbg086"``),
            used to look up the template/catalog schema so partition columns
            (e.g. a Hive-partitioned ``refdate``) are typed correctly instead
            of being inferred as plain strings.
        dataset_path: Absolute path to the dataset folder under db/.
        date_column: Column holding the observation date.
        group_column: Column separating series (e.g. ``symbol``); ``None``
            for single-series datasets.
        group_value: The series value to filter on when ``group_column`` is set.

    Returns:
        Set of distinct dates; empty when the dataset/series is absent or
        unreadable.
    """
    import pyarrow.compute as pc

    if not dataset_path.exists():
        return set()

    dataset = _resolve_series_dataset(
        dataset_key, dataset_path, date_column, group_column
    )
    if dataset is None or date_column not in dataset.schema.names:
        return set()

    filt = None
    if group_column is not None:
        if group_column not in dataset.schema.names:
            return set()
        filt = pc.field(group_column) == group_value

    try:
        table = dataset.to_table(columns=[date_column], filter=filt)
    except Exception:
        return set()

    result: set[date] = set()
    for v in table.column(date_column).to_pylist():
        if v is None:
            continue
        if isinstance(v, str):
            result.add(date.fromisoformat(v))
        elif hasattr(v, "date"):
            result.add(v.date())
        else:
            result.add(v)
    return result


def _read_columns(
    dataset_key: str, dataset_path: Path, columns: list[str]
) -> Any | None:
    """Read the requested columns from a dataset as a pyarrow Table.

    Reuses the typed-schema resolution used by the calendar rules so
    Hive-partitioned columns are typed correctly. Reads only the requested
    columns.

    Args:
        dataset_key: ``"layer.dataset"`` key (for schema lookup).
        dataset_path: Absolute path to the dataset folder under db/.
        columns: Column names to read (non-empty).

    Returns:
        A pyarrow Table of ``columns``, or ``None`` when the dataset is absent.

    Raises:
        KeyError: If any requested column is not present in the schema.
    """
    if not dataset_path.exists():
        return None
    dataset = _resolve_series_dataset(dataset_key, dataset_path, columns[0], None)
    if dataset is None:
        return None
    schema_names = set(dataset.schema.names)
    missing = [c for c in columns if c not in schema_names]
    if missing:
        raise KeyError(f"column(s) not found: {', '.join(missing)}")
    try:
        return dataset.to_table(columns=columns)
    except Exception:
        return None


def _as_date(value: Any) -> date:
    """Coerce a bizdays/datetime/date/ISO-string value to a ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _calendar_completeness_gaps(
    present_dates: set[date],
    calendar_name: str,
    start_cfg: str | None,
    end_cfg: str | None,
) -> list[date]:
    """Return business days missing from a series, sorted ascending.

    Args:
        present_dates: Dates actually present for the series (non-empty).
        calendar_name: bizdays calendar name (e.g. ``ANBIMA``).
        start_cfg: Optional ISO start date; defaults to the first present date.
        end_cfg: Optional ISO end date, or the keyword ``today``; defaults to
            the last present date.

    Returns:
        Sorted list of business days in the window that have no observation.

    Raises:
        Exception: If the calendar name is unknown or start/end are malformed.
    """
    from bizdays import Calendar

    cal = Calendar.load(calendar_name)

    lower = _as_date(start_cfg) if start_cfg is not None else min(present_dates)
    if end_cfg is None:
        upper = max(present_dates)
    elif str(end_cfg) == "today":
        upper = date.today()
    else:
        upper = _as_date(end_cfg)

    # Clamp to the calendar's coverage so bizdays.seq cannot raise out of range.
    lower = max(lower, _as_date(cal.startdate))
    upper = min(upper, _as_date(cal.enddate))
    if lower > upper:
        return []

    biz = [_as_date(d) for d in cal.seq(lower, upper)]
    return sorted(d for d in biz if d not in present_dates)


def _period_completeness_gaps(
    present_dates: set[date],
    frequency: str,
    start_cfg: str | None,
    end_cfg: str | None,
) -> list[str]:
    """Return missing calendar periods for a non-daily series, sorted ascending.

    Calendar-agnostic: a period (month or quarter) is "present" if it holds at
    least one observation. Weekends and holidays are irrelevant to "does this
    period have data", so no bizdays calendar is consulted.

    Args:
        present_dates: Dates actually present for the series (non-empty).
        frequency: Either ``"monthly"`` or ``"quarterly"``.
        start_cfg: Optional ISO start date; defaults to the first present date.
            Bucketed to its period.
        end_cfg: Optional ISO end date, or the keyword ``today``; defaults to
            the last present date. Bucketed to its period.

    Returns:
        Sorted list of period labels (``"YYYY-MM"`` monthly, ``"YYYY-Qn"``
        quarterly) that have no observation.

    Raises:
        ValueError: If ``frequency`` is not ``"monthly"`` or ``"quarterly"``.
        Exception: If ``start_cfg``/``end_cfg`` are malformed dates.
    """
    if frequency not in ("monthly", "quarterly"):
        raise ValueError(f"unsupported frequency: {frequency}")

    def index(d: date) -> int:
        if frequency == "monthly":
            return d.year * 12 + (d.month - 1)
        return d.year * 4 + (d.month - 1) // 3

    def label(i: int) -> str:
        if frequency == "monthly":
            return f"{i // 12:04d}-{i % 12 + 1:02d}"
        return f"{i // 4:04d}-Q{i % 4 + 1}"

    if start_cfg is not None:
        lower = index(_as_date(start_cfg))
    else:
        lower = index(min(present_dates))
    if end_cfg is None:
        upper = index(max(present_dates))
    elif str(end_cfg) == "today":
        upper = index(date.today())
    else:
        upper = index(_as_date(end_cfg))

    present_idx = {index(d) for d in present_dates}
    return [label(i) for i in range(lower, upper + 1) if i not in present_idx]


def _unexpected_observations(
    present_dates: set[date], calendar_name: str
) -> list[date]:
    """Return present dates that fall on non-business days, sorted ascending.

    The inverse of ``_calendar_completeness_gaps``: it flags rows on weekends
    or holidays. Dates outside the calendar's coverage are undeterminable and
    skipped (never flagged), which also guards ``isbizday`` against raising.

    Args:
        present_dates: Dates actually present for the series (non-empty).
        calendar_name: bizdays calendar name (e.g. ``B3``).

    Returns:
        Sorted list of present dates the calendar marks as non-business days.

    Raises:
        Exception: If the calendar name is unknown.
    """
    from bizdays import Calendar

    cal = Calendar.load(calendar_name)
    lower = _as_date(cal.startdate)
    upper = _as_date(cal.enddate)
    return sorted(
        d for d in present_dates if lower <= d <= upper and not cal.isbizday(d)
    )


# ---------------------------------------------------------------------------
# Category: Raw Files
# ---------------------------------------------------------------------------


def check_orphan_raw() -> list[Issue]:
    """Find checksum folders in raw/ not referenced by any metadata row.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager

    man = CacheManager()
    raw_root = Path(man.cache_path(man._raw_folder))

    if not raw_root.exists():
        return []

    # Build set of known (template, checksum) pairs from metadata
    rows = _iter_all_metadata_rows()
    known_checksums: set[tuple[str, str]] = set()
    for row in rows:
        if row["download_checksum"] and row["template"]:
            known_checksums.add((row["template"], row["download_checksum"]))

    # Walk raw/{template}/{checksum} directories
    orphan_dirs: list[str] = []
    for template_dir in raw_root.iterdir():
        if not template_dir.is_dir():
            continue
        for checksum_dir in template_dir.iterdir():
            if not checksum_dir.is_dir():
                continue
            key = (template_dir.name, checksum_dir.name)
            if key not in known_checksums:
                orphan_dirs.append(str(checksum_dir))

    if not orphan_dirs:
        return []

    def _fix() -> None:
        for d in orphan_dirs:
            if Path(d).exists():
                shutil.rmtree(d, ignore_errors=True)

    return [
        Issue(
            category="Raw Files",
            code="orphan-raw",
            severity="warning",
            description=f"{len(orphan_dirs)} orphan raw folder(s) not referenced in metadata",
            details=orphan_dirs,
            fixable=True,
            fix_fn=_fix,
        )
    ]


def check_missing_raw() -> list[Issue]:
    """Find metadata entries referencing non-existent raw file paths.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager

    man = CacheManager()
    rows = _iter_all_metadata_rows()

    missing: list[tuple[str, str]] = []  # (meta_id, path)
    for row in rows:
        if row["is_invalid_download"]:
            continue
        for fpath in row["downloaded_files"]:
            full_path = Path(man.cache_path(fpath))
            if not full_path.exists():
                missing.append((row["id"], fpath))

    if not missing:
        return []

    meta_ids = list({m[0] for m in missing})
    detail_paths = [m[1] for m in missing]

    def _fix() -> None:
        for meta_id in meta_ids:
            _mark_meta_invalid(meta_id)

    return [
        Issue(
            category="Raw Files",
            code="missing-raw",
            severity="error",
            description=f"{len(missing)} metadata entry/entries reference missing raw files",
            details=detail_paths,
            fixable=True,
            fix_fn=_fix,
        )
    ]


# ---------------------------------------------------------------------------
# Category: DB / Parquet
# ---------------------------------------------------------------------------


def check_orphan_db() -> list[Issue]:
    """Find db/ dataset folders with no matching template or catalog entry.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager
    from .layers import DataLayer
    from .template import list_templates, retrieve_template

    man = CacheManager()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        return []

    # Collect known dataset names from catalog
    catalog_rows = _get_catalog_rows()
    catalog_ids: set[str] = {row["id"] for row in catalog_rows}

    # Collect known dataset names from templates (all templates)
    template_datasets: set[str] = set()
    for tname in list_templates():
        try:
            tpl = retrieve_template(tname)
            layer = tpl.writer.layer.value
            dataset_name = tpl.writer.dataset
            template_datasets.add(f"{layer}/{dataset_name}")
            if tpl.datasets:
                for output_name in tpl.datasets:
                    template_datasets.add(f"{layer}/{dataset_name}-{output_name}")
        except Exception:
            continue

    valid_layers = {lay.value for lay in DataLayer if lay != DataLayer.RAW}
    orphan_dirs: list[str] = []

    for layer_dir in db_root.iterdir():
        if not layer_dir.is_dir() or layer_dir.name not in valid_layers:
            continue
        for dataset_dir in layer_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            dataset_id = f"{layer_dir.name}/{dataset_dir.name}"
            if dataset_id not in catalog_ids and dataset_id not in template_datasets:
                orphan_dirs.append(str(dataset_dir))

    if not orphan_dirs:
        return []

    def _fix() -> None:
        for d in orphan_dirs:
            if Path(d).exists():
                shutil.rmtree(d, ignore_errors=True)

    return [
        Issue(
            category="DB / Parquet",
            code="orphan-db",
            severity="warning",
            description=f"{len(orphan_dirs)} DB folder(s) have no matching template or catalog entry",
            details=orphan_dirs,
            fixable=True,
            fix_fn=_fix,
        )
    ]


def check_missing_db() -> list[Issue]:
    """Find templates with processed downloads but missing db folders.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager

    man = CacheManager()
    rows = _iter_all_metadata_rows()

    # Find templates that have at least one processed entry
    processed_templates = {
        row["template"]
        for row in rows
        if row["is_processed"] and not row["is_invalid_download"]
    }

    missing: list[str] = []
    db_root = Path(man.db_path(""))
    for template_id in processed_templates:
        # Check if any parquet files exist for this template across all layers
        found = (
            any(db_root.rglob(f"{template_id}/*.parquet"))
            if db_root.exists()
            else False
        )
        if not found:
            # Also check with wildcard for partitioned datasets
            found = (
                any(db_root.rglob(f"{template_id}/**/*.parquet"))
                if db_root.exists()
                else False
            )
        if not found:
            missing.append(template_id)

    if not missing:
        return []

    return [
        Issue(
            category="DB / Parquet",
            code="missing-db",
            severity="error",
            description=(
                f"{len(missing)} template(s) have processed downloads "
                "but no parquet files found (re-process required)"
            ),
            details=missing,
            fixable=False,
        )
    ]


def check_empty_parquet() -> list[Issue]:
    """Find partition directories that contain no .parquet files.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager
    from .layers import DataLayer

    man = CacheManager()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        return []

    valid_layers = {lay.value for lay in DataLayer if lay != DataLayer.RAW}
    empty_dirs: list[str] = []

    for layer_dir in db_root.iterdir():
        if not layer_dir.is_dir() or layer_dir.name not in valid_layers:
            continue
        for dataset_dir in layer_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            # Look for partition subdirs (e.g. refdate=2024-01-15)
            for part_dir in dataset_dir.iterdir():
                if not part_dir.is_dir():
                    continue
                parquet_files = list(part_dir.glob("*.parquet"))
                if not parquet_files:
                    empty_dirs.append(str(part_dir))

    if not empty_dirs:
        return []

    def _fix() -> None:
        for d in empty_dirs:
            if Path(d).exists():
                shutil.rmtree(d, ignore_errors=True)

    return [
        Issue(
            category="DB / Parquet",
            code="empty-parquet",
            severity="warning",
            description=f"{len(empty_dirs)} empty partition directory/directories (no .parquet files)",
            details=empty_dirs,
            fixable=True,
            fix_fn=_fix,
        )
    ]


def check_corrupted_parquet() -> list[Issue]:
    """Find parquet files that raise an error when read by PyArrow.

    Returns:
        List of issues found.
    """
    import pyarrow.parquet as pq

    from .cache import CacheManager
    from .layers import DataLayer

    man = CacheManager()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        return []

    valid_layers = {lay.value for lay in DataLayer if lay != DataLayer.RAW}
    corrupted: list[str] = []

    for layer_dir in db_root.iterdir():
        if not layer_dir.is_dir() or layer_dir.name not in valid_layers:
            continue
        for parquet_file in layer_dir.rglob("*.parquet"):
            try:
                pq.read_schema(parquet_file)
            except Exception:
                corrupted.append(str(parquet_file))

    if not corrupted:
        return []

    return [
        Issue(
            category="DB / Parquet",
            code="corrupted-parquet",
            severity="error",
            description=(
                f"{len(corrupted)} corrupted parquet file(s) "
                "(manual re-process required)"
            ),
            details=corrupted,
            fixable=False,
        )
    ]


def check_schema_drift() -> list[Issue]:  # noqa: PLR0912
    """Compare dataset_catalog schemas against current template field definitions.

    Returns:
        List of issues found.
    """
    from .catalog import DatasetCatalog
    from .template import retrieve_template

    catalog = DatasetCatalog()
    catalog_rows = _get_catalog_rows()

    drifted: list[str] = []

    for row in catalog_rows:
        source_template = row.get("source_template")
        if not source_template:
            continue
        try:
            tpl = retrieve_template(source_template)
        except Exception:
            continue

        # Build expected field names from template
        try:
            if tpl.fields:
                expected_names = set(tpl.fields.get_field_names())
            elif tpl.datasets:
                # Multi-output: collect all field names across datasets
                expected_names = set()
                for ds_config in tpl.datasets.values():
                    expected_names.update(ds_config.fields.get_field_names())
            else:
                continue
        except Exception:
            continue

        # Get stored schema field names
        try:
            info = catalog.get_dataset_info(row["layer"], row["dataset_name"])
            if info is None:
                continue
            catalog_names = {f.name for f in info.schema}
        except Exception:
            continue

        # Compare — report if the sets differ
        only_in_catalog = catalog_names - expected_names
        only_in_template = expected_names - catalog_names

        if only_in_catalog or only_in_template:
            detail = (
                f"{row['layer']}.{row['dataset_name']} (template: {source_template})"
            )
            if only_in_catalog:
                detail += f"; in catalog only: {sorted(only_in_catalog)}"
            if only_in_template:
                detail += f"; in template only: {sorted(only_in_template)}"
            drifted.append(detail)

    if not drifted:
        return []

    return [
        Issue(
            category="DB / Parquet",
            code="schema-drift",
            severity="warning",
            description=f"{len(drifted)} dataset(s) have schema drift vs. template definition",
            details=drifted,
            fixable=False,
        )
    ]


# ---------------------------------------------------------------------------
# Category: Metadata
# ---------------------------------------------------------------------------


def check_unresolved_errors() -> list[Issue]:
    """Find cache_metadata rows with non-empty processing_errors.

    Returns:
        List of issues found.
    """
    rows = _iter_all_metadata_rows()
    errored: list[str] = []

    for row in rows:
        if row["processing_errors"]:
            errored.append(
                f"{row['template']} (id={row['id'][:8]}…): {row['processing_errors'][:80]}"
            )

    if not errored:
        return []

    return [
        Issue(
            category="Metadata",
            code="unresolved-errors",
            severity="warning",
            description=f"{len(errored)} metadata row(s) have unresolved processing errors",
            details=errored,
            fixable=False,
        )
    ]


def check_invalid_downloads() -> list[Issue]:
    """Find cache_metadata rows marked as invalid downloads.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager

    man = CacheManager()
    rows = _iter_all_metadata_rows()
    invalid_rows: list[dict] = [r for r in rows if r["is_invalid_download"]]

    if not invalid_rows:
        return []

    details = [
        f"{r['template']} checksum={r['download_checksum'] or 'none'} (id={r['id'][:8]}…)"
        for r in invalid_rows
    ]
    meta_ids = [r["id"] for r in invalid_rows]

    def _fix() -> None:
        for row in invalid_rows:
            # Delete raw folder if it exists
            checksum = row["download_checksum"]
            template = row["template"]
            if checksum and template:
                raw_dir = Path(man.cache_path(man._raw_folder)) / template / checksum
                if raw_dir.exists():
                    shutil.rmtree(raw_dir, ignore_errors=True)
        # Delete metadata rows
        for meta_id in meta_ids:
            _delete_meta_row(meta_id)

    return [
        Issue(
            category="Metadata",
            code="invalid-downloads",
            severity="warning",
            description=f"{len(invalid_rows)} metadata row(s) marked as invalid downloads",
            details=details,
            fixable=True,
            fix_fn=_fix,
        )
    ]


# ---------------------------------------------------------------------------
# Category: Template Consistency
# ---------------------------------------------------------------------------


def check_stale_etl(template_filter: list[str] | None = None) -> list[Issue]:
    """Find ETL outputs older than their source input datasets.

    Args:
        template_filter: If given, only check these template IDs.

    Returns:
        List of issues found.
    """
    catalog_rows = _get_catalog_rows()
    if not catalog_rows:
        return []

    # Build lookup: dataset_id -> updated_at
    updated_at: dict[str, datetime] = {}
    for row in catalog_rows:
        with suppress(Exception):
            updated_at[row["id"]] = datetime.fromisoformat(row["updated_at"])

    stale: list[str] = []

    # Check each catalog entry that has a source_template and is staging/curated
    for row in catalog_rows:
        source_template = row.get("source_template")
        layer = row.get("layer")
        if not source_template or layer not in ("staging", "curated"):
            continue
        if template_filter and source_template not in template_filter:
            continue

        try:
            from .template import retrieve_template

            tpl = retrieve_template(source_template)
            if not tpl.is_etl:
                continue
            input_datasets = tpl.etl.get_input_datasets()
        except Exception:
            continue

        etl_updated = updated_at.get(row["id"])
        if etl_updated is None:
            continue

        for input_ds in input_datasets:
            # input_ds may be "dataset-name" without layer prefix; assume input layer
            input_ds_id = f"input/{input_ds}" if "/" not in input_ds else input_ds

            input_updated = updated_at.get(input_ds_id)
            if input_updated is None:
                continue

            if input_updated > etl_updated:
                delta = input_updated - etl_updated
                stale.append(
                    f"{row['layer']}.{row['dataset_name']} "
                    f"(source: {source_template}, "
                    f"input updated {delta.days}d ago)"
                )
                break

    if not stale:
        return []

    return [
        Issue(
            category="Template Consistency",
            code="stale-etl",
            severity="warning",
            description=f"{len(stale)} ETL output(s) are older than their source input dataset(s)",
            details=stale,
            fixable=False,
        )
    ]


def check_missing_etl_source(template_filter: list[str] | None = None) -> list[Issue]:
    """Find ETL templates referencing upstream datasets with no data on disk.

    Args:
        template_filter: If given, only check these template IDs.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager
    from .template import list_templates, retrieve_template

    man = CacheManager()
    missing: list[str] = []

    for tname in list_templates():
        if template_filter and tname not in template_filter:
            continue
        try:
            tpl = retrieve_template(tname)
            if not tpl.is_etl:
                continue
            input_datasets = tpl.etl.get_input_datasets()
        except Exception:
            continue

        for input_ds in input_datasets:
            # Assume input layer if no slash prefix
            if "/" not in input_ds:
                ds_path = Path(man.db_path(f"input/{input_ds}"))
            else:
                ds_path = Path(man.db_path(input_ds))

            if not ds_path.exists() or not any(ds_path.rglob("*.parquet")):
                missing.append(f"{tname} → {input_ds} (no data on disk)")

    if not missing:
        return []

    return [
        Issue(
            category="Template Consistency",
            code="missing-etl-source",
            severity="info",
            description=f"{len(missing)} ETL template(s) reference upstream dataset(s) with no data on disk",
            details=missing,
            fixable=False,
        )
    ]


# ---------------------------------------------------------------------------
# Category: Date Gaps
# ---------------------------------------------------------------------------


def _parse_refdate_partition(dir_name: str) -> date | None:
    """Parse a date from a Hive-style partition folder name like refdate=2024-01-15.

    Args:
        dir_name: Directory name (e.g. "refdate=2024-01-15").

    Returns:
        Parsed date, or None if not a refdate partition.
    """
    if not dir_name.startswith("refdate="):
        return None
    try:
        return date.fromisoformat(dir_name[len("refdate=") :])
    except ValueError:
        return None


def _cutoff_from_last_days(last_days: int) -> date:
    """Return the earliest date of the last_days window; full history when < 0."""
    if last_days < 0:
        return date.min
    return date.fromordinal(date.today().toordinal() - last_days)


def check_date_gaps(  # noqa: PLR0912, PLR0915
    last_days: int = 30,
    template_filter: list[str] | None = None,
    calendar_name: str = "B3",
) -> list[Issue]:
    """Find time-series datasets with missing business days.

    Args:
        last_days: Only look back this many calendar days from today;
            a negative value (-1) reviews the full history.
        template_filter: If given, only check datasets produced by these templates.
        calendar_name: bizdays calendar name (default ``B3``).

    Returns:
        List of issues found (always includes a ``date-gaps-coverage`` info
        per evaluated dataset).
    """
    try:
        from bizdays import Calendar

        Calendar.load(calendar_name)
    except Exception:
        return []

    from .cache import CacheManager
    from .layers import DataLayer

    man = CacheManager()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        return []

    cutoff_date = _cutoff_from_last_days(last_days)

    catalog_rows = _get_catalog_rows()
    # Build source_template lookup: dataset_id -> source_template
    template_by_dataset: dict[str, str] = {}
    for row in catalog_rows:
        if row.get("source_template"):
            template_by_dataset[row["id"]] = row["source_template"]

    valid_layers = {lay.value for lay in DataLayer if lay != DataLayer.RAW}
    issues: list[Issue] = []

    for layer_dir in db_root.iterdir():
        if not layer_dir.is_dir() or layer_dir.name not in valid_layers:
            continue
        for dataset_dir in layer_dir.iterdir():
            if not dataset_dir.is_dir():
                continue

            dataset_id = f"{layer_dir.name}/{dataset_dir.name}"

            # Apply template filter if specified
            if template_filter:
                src_tpl = template_by_dataset.get(dataset_id)
                if src_tpl not in template_filter:
                    continue

            # Collect all refdate partition values
            refdate_values: list[date] = []
            for child in dataset_dir.iterdir():
                if not child.is_dir():
                    continue
                parsed = _parse_refdate_partition(child.name)
                if parsed is not None:
                    refdate_values.append(parsed)

            if len(refdate_values) < 2:
                continue

            refdate_values.sort()
            first_date = max(refdate_values[0], cutoff_date)
            last_date = refdate_values[-1]

            if first_date > last_date:
                # Dates exist, but all fall before the --last cutoff.
                issues.append(
                    Issue(
                        category="Date Gaps",
                        code="date-gaps-coverage",
                        severity="info",
                        description=(
                            f"{dataset_id}: no dates within the evaluated "
                            f"window (last {last_days} days; most recent date "
                            f"{last_date})"
                        ),
                        details=[],
                        fixable=False,
                    )
                )
                continue

            present_dates = set(refdate_values)
            try:
                missing_days = _calendar_completeness_gaps(
                    present_dates,
                    calendar_name,
                    first_date.isoformat(),
                    last_date.isoformat(),
                )
                extra_days = [
                    d
                    for d in _unexpected_observations(present_dates, calendar_name)
                    if first_date <= d <= last_date
                ]
            except Exception:
                continue

            in_window = sum(1 for d in present_dates if first_date <= d <= last_date)
            observed = in_window - len(extra_days)
            expected = observed + len(missing_days)
            issues.append(
                Issue(
                    category="Date Gaps",
                    code="date-gaps-coverage",
                    severity="info",
                    description=(
                        f"{dataset_id}: checked {first_date} → {last_date} "
                        f"({observed}/{expected} {calendar_name} business days "
                        f"present)"
                    ),
                    details=[],
                    fixable=False,
                )
            )

            if not missing_days:
                continue

            issues.append(
                Issue(
                    category="Date Gaps",
                    code="date-gaps",
                    severity="error",
                    description=(
                        f"{dataset_dir.name}: {len(missing_days)} missing "
                        f"{calendar_name} business day(s) "
                        f"between {first_date} and {last_date}"
                    ),
                    details=[str(d) for d in missing_days],
                    fixable=False,
                )
            )

    return issues


# ---------------------------------------------------------------------------
# Category: Data Validation
# ---------------------------------------------------------------------------


def _validation_config_error(dataset_key: str, msg: str) -> Issue:
    """Build a config-error Issue for a malformed validation entry."""
    return Issue(
        category="Data Validation",
        code="validation-config-error",
        severity="error",
        description=f"Malformed validation config for {dataset_key}: {msg}",
        details=[],
        fixable=False,
    )


def _expand_series_items(
    dataset_key: str, rule: dict[str, Any]
) -> tuple[list[tuple[str | None, dict[str, Any]]] | None, Issue | None]:
    """Expand a validation rule into ``(series_name, series_cfg)`` items.

    Grouped rules (``group_column`` set) draw one item per entry in the
    ``series:`` map; single-series rules yield one ``(None, rule)`` item.

    Args:
        dataset_key: ``"layer.dataset"`` key, used only for error messages.
        rule: The rule mapping from the validations config.

    Returns:
        ``(series_items, None)`` on success, or ``(None, error_issue)`` when a
        grouped rule is missing a non-empty ``series:`` map.
    """
    group_column = rule.get("group_column")
    if group_column is not None:
        series = rule.get("series")
        if not isinstance(series, dict) or not series:
            return None, _validation_config_error(
                dataset_key, "grouped rule requires a non-empty 'series' map"
            )
        return [
            (name, cfg if isinstance(cfg, dict) else {}) for name, cfg in series.items()
        ], None
    return [(None, rule)], None


def _run_calendar_completeness_rule(
    dataset_key: str, dataset_path: Path, rule: dict[str, Any]
) -> list[Issue]:
    """Evaluate a single calendar-completeness rule for one dataset."""
    date_column = rule.get("date_column", "refdate")
    group_column = rule.get("group_column")

    series_items, err = _expand_series_items(dataset_key, rule)
    if err is not None:
        return [err]

    issues: list[Issue] = []
    for series_name, scfg in series_items:
        calendar_name = scfg.get("calendar", "ANBIMA")
        start_cfg = scfg.get("start")
        end_cfg = scfg.get("end")
        frequency = scfg.get("frequency", "daily")
        label = f"{dataset_key}/{series_name}" if series_name else dataset_key

        if frequency not in ("daily", "monthly", "quarterly"):
            issues.append(
                _validation_config_error(
                    label,
                    f"unknown frequency '{frequency}' "
                    "(expected daily, monthly, or quarterly)",
                )
            )
            continue

        present = _read_series_dates(
            dataset_key, dataset_path, date_column, group_column, series_name
        )
        if not present:
            continue  # absent dataset/series -> skip silently

        missing: list[Any]
        try:
            if frequency == "daily":
                missing = _calendar_completeness_gaps(
                    present, calendar_name, start_cfg, end_cfg
                )
                noun = f"{calendar_name} business day"
            elif frequency == "monthly":
                missing = _period_completeness_gaps(
                    present, "monthly", start_cfg, end_cfg
                )
                noun = "month"
            else:  # quarterly
                missing = _period_completeness_gaps(
                    present, "quarterly", start_cfg, end_cfg
                )
                noun = "quarter"
        except Exception as exc:
            issues.append(
                _validation_config_error(
                    label, f"invalid calendar-completeness config ({exc})"
                )
            )
            continue

        if missing:
            issues.append(
                Issue(
                    category="Data Validation",
                    code="calendar-completeness",
                    severity="error",
                    description=(
                        f"{label}: {len(missing)} missing {noun}(s) "
                        f"between {missing[0]} and {missing[-1]}"
                    ),
                    details=[str(d) for d in missing],
                    fixable=False,
                )
            )
    return issues


def _run_no_unexpected_observations_rule(
    dataset_key: str, dataset_path: Path, rule: dict[str, Any]
) -> list[Issue]:
    """Evaluate a single no-unexpected-observations rule for one dataset."""
    date_column = rule.get("date_column", "refdate")
    group_column = rule.get("group_column")

    series_items, err = _expand_series_items(dataset_key, rule)
    if err is not None:
        return [err]

    issues: list[Issue] = []
    for series_name, scfg in series_items:
        if scfg.get("frequency", "daily") != "daily":
            continue  # non-daily series: rule is meaningless -> skip silently
        calendar_name = scfg.get("calendar", "ANBIMA")
        label = f"{dataset_key}/{series_name}" if series_name else dataset_key

        present = _read_series_dates(
            dataset_key, dataset_path, date_column, group_column, series_name
        )
        if not present:
            continue  # absent dataset/series -> skip silently

        try:
            unexpected = _unexpected_observations(present, calendar_name)
        except Exception as exc:
            issues.append(
                _validation_config_error(
                    label, f"invalid no-unexpected-observations config ({exc})"
                )
            )
            continue

        if unexpected:
            issues.append(
                Issue(
                    category="Data Validation",
                    code="no-unexpected-observations",
                    severity="error",
                    description=(
                        f"{label}: {len(unexpected)} observation(s) on non-business "
                        f"days ({calendar_name}) between "
                        f"{unexpected[0]} and {unexpected[-1]}"
                    ),
                    details=[str(d) for d in unexpected],
                    fixable=False,
                )
            )
    return issues


def _run_value_range_rule(  # noqa: PLR0911
    dataset_key: str, dataset_path: Path, rule: dict[str, Any]
) -> list[Issue]:
    """Evaluate a value-range rule: a numeric column within [min, max]."""
    import pyarrow as pa
    import pyarrow.compute as pc

    column = rule.get("column")
    if not isinstance(column, str) or not column:
        return [
            _validation_config_error(dataset_key, "value-range requires a 'column'")
        ]
    min_v = rule.get("min")
    max_v = rule.get("max")
    if min_v is None and max_v is None:
        return [
            _validation_config_error(
                dataset_key, "value-range requires 'min' and/or 'max'"
            )
        ]

    try:
        table = _read_columns(dataset_key, dataset_path, [column])
    except KeyError as exc:
        return [_validation_config_error(dataset_key, str(exc))]
    if table is None:
        return []

    col = table.column(column)
    if not (pa.types.is_floating(col.type) or pa.types.is_integer(col.type)):
        return [
            _validation_config_error(
                dataset_key, f"value-range column '{column}' is not numeric"
            )
        ]

    valid = pc.is_valid(col)
    below = pc.less(col, min_v) if min_v is not None else None
    above = pc.greater(col, max_v) if max_v is not None else None
    if below is not None and above is not None:
        out_of_range = pc.or_(below, above)
    elif below is not None:
        out_of_range = below
    else:
        out_of_range = above

    offending = col.filter(pc.and_(valid, out_of_range))
    n = len(offending)
    if n == 0:
        return []

    valid_vals = col.filter(valid)
    obs_min = pc.min(valid_vals).as_py()
    obs_max = pc.max(valid_vals).as_py()
    sample = [str(v) for v in offending.slice(0, 20).to_pylist()]
    return [
        Issue(
            category="Data Validation",
            code="value-range",
            severity="error",
            description=(
                f"{dataset_key}: {n} row(s) with {column} outside "
                f"[{min_v}, {max_v}] (observed [{obs_min}, {obs_max}])"
            ),
            details=sample,
            fixable=False,
        )
    ]


def _run_not_null_rule(
    dataset_key: str, dataset_path: Path, rule: dict[str, Any]
) -> list[Issue]:
    """Evaluate a not-null rule: configured columns must have no nulls."""
    columns = rule.get("columns")
    if not isinstance(columns, list) or not columns:
        return [
            _validation_config_error(
                dataset_key, "not-null requires a non-empty 'columns' list"
            )
        ]

    try:
        table = _read_columns(dataset_key, dataset_path, columns)
    except KeyError as exc:
        return [_validation_config_error(dataset_key, str(exc))]
    if table is None:
        return []

    violating = [(c, table.column(c).null_count) for c in columns]
    violating = [(c, n) for c, n in violating if n > 0]
    if not violating:
        return []
    return [
        Issue(
            category="Data Validation",
            code="not-null",
            severity="error",
            description=(
                f"{dataset_key}: null values in {len(violating)} required column(s)"
            ),
            details=[f"{c}: {n} null(s)" for c, n in violating],
            fixable=False,
        )
    ]


def _run_no_duplicates_rule(
    dataset_key: str, dataset_path: Path, rule: dict[str, Any]
) -> list[Issue]:
    """Evaluate a no-duplicates rule: no duplicate rows for the key columns."""
    import pyarrow.compute as pc

    key = rule.get("key")
    if not isinstance(key, list) or not key:
        return [
            _validation_config_error(
                dataset_key, "no-duplicates requires a non-empty 'key' list"
            )
        ]

    try:
        table = _read_columns(dataset_key, dataset_path, key)
    except KeyError as exc:
        return [_validation_config_error(dataset_key, str(exc))]
    if table is None:
        return []

    grouped = table.group_by(key).aggregate([([], "count_all")])
    dup = grouped.filter(pc.greater(grouped.column("count_all"), 1))
    n_dup_keys = dup.num_rows
    if n_dup_keys == 0:
        return []

    excess = pc.sum(dup.column("count_all")).as_py() - n_dup_keys
    details = []
    for row in dup.slice(0, 20).to_pylist():
        keyvals = ", ".join(str(row[k]) for k in key)
        details.append(f"({keyvals}): {row['count_all']} rows")
    return [
        Issue(
            category="Data Validation",
            code="no-duplicates",
            severity="error",
            description=(
                f"{dataset_key}: {n_dup_keys} duplicated key(s) on "
                f"[{', '.join(key)}] ({excess} excess row(s))"
            ),
            details=details,
            fixable=False,
        )
    ]


def check_validations(
    validations_config: Path,
) -> list[Issue]:
    """Run all configured validation rules against the stored datasets.

    Loads the validations spec and dispatches each rule by its ``rule:``
    discriminator (``calendar-completeness``, ``no-unexpected-observations``).

    Args:
        validations_config: Path to the validations YAML file. Required;
            ``run_doctor`` guarantees a non-None path before calling.

    Returns:
        List of issues (findings and config errors). Never raises.
    """
    from .cache import CacheManager

    man = CacheManager()

    try:
        config = _load_validations_config(validations_config)
    except Exception as exc:
        return [
            Issue(
                category="Data Validation",
                code="validation-config-error",
                severity="error",
                description=f"Cannot load validations config: {exc}",
                details=[],
                fixable=False,
            )
        ]

    issues: list[Issue] = []
    for dataset_key, rules in config.items():
        if not isinstance(rules, list):
            issues.append(_validation_config_error(dataset_key, "rules must be a list"))
            continue
        dataset_path = Path(man.db_path(dataset_key.replace(".", "/", 1)))
        for rule in rules:
            if not isinstance(rule, dict):
                issues.append(
                    _validation_config_error(dataset_key, "rule must be a mapping")
                )
                continue
            rule_type = rule.get("rule")
            if rule_type == "calendar-completeness":
                issues.extend(
                    _run_calendar_completeness_rule(dataset_key, dataset_path, rule)
                )
            elif rule_type == "no-unexpected-observations":
                issues.extend(
                    _run_no_unexpected_observations_rule(
                        dataset_key, dataset_path, rule
                    )
                )
            elif rule_type == "value-range":
                issues.extend(_run_value_range_rule(dataset_key, dataset_path, rule))
            elif rule_type == "not-null":
                issues.extend(_run_not_null_rule(dataset_key, dataset_path, rule))
            elif rule_type == "no-duplicates":
                issues.extend(_run_no_duplicates_rule(dataset_key, dataset_path, rule))
            else:
                issues.append(
                    _validation_config_error(
                        dataset_key, f"unknown validation rule '{rule_type}'"
                    )
                )
    return issues


def check_download_refdate_gaps(
    template_filter: list[str] | None,
    calendar_name: str,
    last_days: int = 30,
) -> list[Issue]:
    """Verify downloaded refdate coverage for named templates (from meta).

    Reads ``cache_metadata`` for each template in ``template_filter``, collects
    the ``refdate`` values from ``download_args`` (skipping invalid downloads),
    and compares the observed window — clamped to the last ``last_days``
    calendar days, mirroring ``check_date_gaps`` — against the given business
    calendar. Silent when no ``template_filter`` is given.

    Args:
        template_filter: Templates to check. ``None``/empty yields no issues.
        calendar_name: bizdays calendar name (e.g. ``B3``).
        last_days: Only look back this many calendar days from today;
            a negative value (-1) reviews the full history.

    Returns:
        Per template: a ``download-refdate-coverage`` info (always, when any
        refdate exists), plus a ``download-refdate-gaps`` error and a
        ``download-refdate-extra`` info when applicable; or a single
        ``download-refdate-missing-template`` warning. Never fixable.
    """
    if not template_filter:
        return []

    placeholders = ",".join("?" * len(template_filter))
    with closing(_get_meta_connection()) as conn, conn:
        c = conn.cursor()
        c.execute(
            "SELECT template, download_args, is_invalid_download "
            f"FROM cache_metadata WHERE template IN ({placeholders})",
            template_filter,
        )
        rows = c.fetchall()

    present: dict[str, set[date]] = {t: set() for t in template_filter}
    for template, download_args, is_invalid in rows:
        if is_invalid == "1":
            continue
        try:
            args = json.loads(download_args or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        refdate_raw = args.get("refdate") if isinstance(args, dict) else None
        if not refdate_raw:
            continue
        try:
            present[template].add(datetime.fromisoformat(str(refdate_raw)).date())
        except ValueError:
            continue

    cutoff_date = _cutoff_from_last_days(last_days)

    issues: list[Issue] = []
    for template in template_filter:
        dates = present[template]
        if not dates:
            issues.append(
                Issue(
                    category="Downloads",
                    code="download-refdate-missing-template",
                    severity="warning",
                    description=(
                        f"{template}: no downloaded refdates found in metadata"
                    ),
                    details=[],
                    fixable=False,
                )
            )
            continue

        first_date = max(min(dates), cutoff_date)
        last_date = max(dates)
        if first_date > last_date:
            # Dates exist, but all fall before the --last cutoff.
            issues.append(
                Issue(
                    category="Downloads",
                    code="download-refdate-coverage",
                    severity="info",
                    description=(
                        f"{template}: no downloaded dates within the evaluated "
                        f"window (last {last_days} days; most recent download "
                        f"{last_date})"
                    ),
                    details=[],
                    fixable=False,
                )
            )
            continue

        missing = _calendar_completeness_gaps(
            dates, calendar_name, first_date.isoformat(), last_date.isoformat()
        )
        extra = [
            d
            for d in _unexpected_observations(dates, calendar_name)
            if first_date <= d <= last_date
        ]
        in_window = sum(1 for d in dates if first_date <= d <= last_date)
        observed = in_window - len(extra)
        expected = observed + len(missing)
        issues.append(
            Issue(
                category="Downloads",
                code="download-refdate-coverage",
                severity="info",
                description=(
                    f"{template}: checked {first_date} → {last_date} "
                    f"({observed}/{expected} {calendar_name} business days "
                    f"downloaded)"
                ),
                details=[],
                fixable=False,
            )
        )
        if missing:
            issues.append(
                Issue(
                    category="Downloads",
                    code="download-refdate-gaps",
                    severity="error",
                    description=(
                        f"{template}: {len(missing)} missing {calendar_name} "
                        f"business day(s) between {missing[0]} and {missing[-1]}"
                    ),
                    details=[str(d) for d in missing],
                    fixable=False,
                )
            )

        if extra:
            issues.append(
                Issue(
                    category="Downloads",
                    code="download-refdate-extra",
                    severity="info",
                    description=(
                        f"{template}: {len(extra)} downloaded date(s) not on the "
                        f"{calendar_name} calendar between {extra[0]} and {extra[-1]}"
                    ),
                    details=[str(d) for d in extra],
                    fixable=False,
                )
            )
    return issues


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_CATEGORY_KEYS = {
    "raw": ["orphan-raw", "missing-raw"],
    "db": [
        "orphan-db",
        "missing-db",
        "empty-parquet",
        "corrupted-parquet",
        "schema-drift",
    ],
    "meta": ["unresolved-errors", "invalid-downloads"],
    "templates": ["stale-etl", "missing-etl-source"],
    "gaps": ["date-gaps"],
    "validations": ["validations"],
    "downloads": ["download-refdate-gaps"],
}


def run_doctor(
    categories: list[str] | None = None,
    template_filter: list[str] | None = None,
    last_days: int = 30,
    validations_config: Path | None = None,
    calendar_name: str = "B3",
) -> DoctorReport:
    """Run all (or selected) diagnostic checks and return a DoctorReport.

    Args:
        categories: Optional list of category keys to restrict checks.
            Valid values: "raw", "db", "meta", "templates", "gaps",
            "validations", "downloads". If None, all checks are run.
        template_filter: Restrict the date-gaps, stale-etl, missing-etl-source
            and downloads checks to these template IDs.
        last_days: For the gaps and downloads categories, only look back this
            many days; a negative value (-1) reviews the full history.
        validations_config: Path to a validations YAML file. Required for the
            "validations" category. If None and "validations" is explicitly
            requested, a ValueError is raised; if None during a broad run, the
            validation checks are skipped with an informational note.
        calendar_name: Business calendar for the gaps and downloads
            categories (default "B3").

    Returns:
        DoctorReport aggregating all issues found.

    Raises:
        ValueError: If "validations" is explicitly requested without
            validations_config.
    """
    report = DoctorReport()

    # Determine which category codes to run
    if categories:
        active_codes: set[str] = set()
        for cat_key in categories:
            active_codes.update(_CATEGORY_KEYS.get(cat_key, []))
    else:
        active_codes = {code for codes in _CATEGORY_KEYS.values() for code in codes}

    # Validations require an explicit spec file (WIL-91). With no file, an
    # explicit `--category validations` is a usage error, while a broad run
    # skips the validation checks with an informational note. Generic over the
    # whole validations category so future rule types inherit this policy.
    validation_codes = set(_CATEGORY_KEYS["validations"])
    explicit_validations = categories is not None and "validations" in categories
    if validations_config is None and (active_codes & validation_codes):
        if explicit_validations:
            raise ValueError("the 'validations' category requires --validations-file")
        active_codes -= validation_codes
        report.issues.append(
            Issue(
                category="Data Validation",
                code="validations-skipped",
                severity="info",
                description="validations skipped: no --validations-file provided",
                details=[],
                fixable=False,
            )
        )

    check_map: dict[str, Callable[[], list[Issue]]] = {
        "orphan-raw": check_orphan_raw,
        "missing-raw": check_missing_raw,
        "orphan-db": check_orphan_db,
        "missing-db": check_missing_db,
        "empty-parquet": check_empty_parquet,
        "corrupted-parquet": check_corrupted_parquet,
        "schema-drift": check_schema_drift,
        "unresolved-errors": check_unresolved_errors,
        "invalid-downloads": check_invalid_downloads,
        "stale-etl": lambda: check_stale_etl(template_filter),
        "missing-etl-source": lambda: check_missing_etl_source(template_filter),
        "date-gaps": lambda: check_date_gaps(last_days, template_filter, calendar_name),
        "download-refdate-gaps": lambda: check_download_refdate_gaps(
            template_filter, calendar_name, last_days
        ),
    }
    if validations_config is not None:
        check_map["validations"] = lambda: check_validations(validations_config)

    for code, check_fn in check_map.items():
        if code not in active_codes:
            continue
        try:
            issues = check_fn()
            report.issues.extend(issues)
        except Exception as exc:
            # A failing check should not abort the whole doctor run
            report.issues.append(
                Issue(
                    category="Internal",
                    code=f"check-failed-{code}",
                    severity="warning",
                    description=f"Check '{code}' raised an exception: {exc}",
                    details=[],
                    fixable=False,
                )
            )

    return report
