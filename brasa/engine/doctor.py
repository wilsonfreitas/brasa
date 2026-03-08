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


def check_date_gaps(  # noqa: PLR0912
    since_days: int = 30,
    template_filter: list[str] | None = None,
) -> list[Issue]:
    """Find time-series datasets with missing B3 business days.

    Args:
        since_days: Only look back this many calendar days from today.
        template_filter: If given, only check datasets produced by these templates.

    Returns:
        List of issues found.
    """
    try:
        from bizdays import Calendar

        cal = Calendar.load("B3")
    except Exception:
        return []

    from .cache import CacheManager
    from .layers import DataLayer

    man = CacheManager()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        return []

    today = date.today()
    cutoff_date = date.fromordinal(today.toordinal() - since_days)

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

            if first_date >= last_date:
                continue

            # Get B3 business days in range
            try:
                biz_days: list[date] = [
                    d.date() if hasattr(d, "date") else d
                    for d in cal.seq(first_date, last_date)
                ]
            except Exception:
                continue

            present_dates = set(refdate_values)
            missing_days = [d for d in biz_days if d not in present_dates]

            if not missing_days:
                continue

            issues.append(
                Issue(
                    category="Date Gaps",
                    code="date-gaps",
                    severity="error",
                    description=(
                        f"{dataset_dir.name}: {len(missing_days)} missing B3 business day(s) "
                        f"between {first_date} and {last_date}"
                    ),
                    details=[str(d) for d in missing_days],
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
}


def run_doctor(
    categories: list[str] | None = None,
    template_filter: list[str] | None = None,
    since_days: int = 30,
) -> DoctorReport:
    """Run all (or selected) diagnostic checks and return a DoctorReport.

    Args:
        categories: Optional list of category keys to restrict checks.
            Valid values: "raw", "db", "meta", "templates", "gaps".
            If None, all checks are run.
        template_filter: Restrict date-gap and stale-etl checks to these
            template IDs.
        since_days: For date-gap checks, only look back this many days.

    Returns:
        DoctorReport aggregating all issues found.
    """
    report = DoctorReport()

    # Determine which category codes to run
    if categories:
        active_codes: set[str] = set()
        for cat_key in categories:
            active_codes.update(_CATEGORY_KEYS.get(cat_key, []))
    else:
        active_codes = {code for codes in _CATEGORY_KEYS.values() for code in codes}

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
        "date-gaps": lambda: check_date_gaps(since_days, template_filter),
    }

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
