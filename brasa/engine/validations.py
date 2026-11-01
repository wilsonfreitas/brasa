"""Dataset validation rules for ``brasa validate``.

Rules check the *data* stored in the datalake (not the template files). They
are collected from inline ``validations:`` blocks in templates, from
``validations/`` files in template folders and from an optional ``--rules``
file, then run per ``layer.dataset``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml

from .doctor import (
    DoctorReport,
    Issue,
    _as_date,
    _calendar_completeness_gaps,
    _unexpected_observations,
)


def _skipped(label: str, dataset_path: Path, series_name: str | None) -> Issue:
    """Build the info issue for a rule that found nothing to check.

    Skips are reported instead of silently ignored, so a rule pointing at a
    missing series (a typo) is visible, while a dataset that was never
    downloaded does not fail the run.
    """
    if not dataset_path.exists():
        reason = "dataset not in the datalake"
    elif series_name is not None:
        reason = f"series '{series_name}' not found"
    else:
        reason = "no rows to check"
    return Issue(
        category="Data Validation",
        code="validation-skipped",
        severity="info",
        description=f"skipped: {label} — {reason}",
    )


def load_rules_file(path: Path) -> dict[str, Any]:
    """Load a rules file: a mapping of ``layer.dataset`` -> list of rules.

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
            issues.append(_skipped(label, dataset_path, series_name))
            continue

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
            issues.append(_skipped(label, dataset_path, series_name))
            continue

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
        return [_skipped(dataset_key, dataset_path, None)]

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
        return [_skipped(dataset_key, dataset_path, None)]

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
        return [_skipped(dataset_key, dataset_path, None)]

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


def run_rules(index: dict[str, Any]) -> list[Issue]:
    """Run validation rules against the stored datasets.

    Each rule is dispatched by its ``rule:`` discriminator
    (``calendar-completeness``, ``no-unexpected-observations``,
    ``value-range``, ``not-null``, ``no-duplicates``).

    Args:
        index: Mapping of ``"layer.dataset"`` to its list of rules.

    Returns:
        Findings, skipped rules (info) and config errors. Never raises for a
        malformed rule.
    """
    from .cache import CacheManager

    man = CacheManager()

    issues: list[Issue] = []
    for dataset_key, rules in index.items():
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


def _config_error(description: str) -> Issue:
    """Build an error issue for a rule definition problem."""
    return Issue(
        category="Data Validation",
        code="validation-config-error",
        severity="error",
        description=description,
    )


def _add_rules(
    index: dict[str, list[Any]],
    key: str,
    rules: Any,
    source: str,
    problems: list[Issue],
) -> None:
    """Append ``rules`` to ``index[key]``, or record a problem if not a list."""
    if not isinstance(rules, list):
        problems.append(_config_error(f"{source}: rules for {key} must be a list"))
        return
    index.setdefault(key, []).extend(rules)


def collect_rules(
    rules_file: Path | None = None,
) -> tuple[dict[str, list[Any]], set[str], list[Issue]]:
    """Build the per-dataset rule index.

    Sources, merged as a union (nothing overrides anything): the inline
    ``validations:`` blocks of every discovered template, every
    ``validations/*.yaml`` file in each template folder, and ``rules_file``.

    Args:
        rules_file: Optional extra rules file (same format as folder files).

    Returns:
        ``(index, known_datasets, problems)``: the rules per ``layer.dataset``,
        every dataset that some template produces, and the rule definition
        problems found while collecting.
    """
    from .dependency_graph import TemplateDependencyGraph
    from .template import get_template_folders

    graph = TemplateDependencyGraph()
    known = {
        out.dataset_id.replace("/", ".", 1)
        for outs in graph.outputs.values()
        for out in outs
    }
    index: dict[str, list[Any]] = {}
    problems: list[Issue] = []

    for tid, tpl in graph.templates.items():
        top = getattr(tpl, "validations", None)
        layer = tpl.writer.layer.value
        source = f"template {tid}"
        if tpl.datasets:
            if top is not None:
                problems.append(
                    _config_error(
                        f"{source}: it writes several datasets, so put "
                        "'validations:' inside each 'datasets:' entry"
                    )
                )
            for key, dataset in tpl.datasets.items():
                if dataset.validations is not None:
                    _add_rules(
                        index,
                        f"{layer}.{tid}-{key}",
                        dataset.validations,
                        source,
                        problems,
                    )
        elif top is not None:
            _add_rules(index, f"{layer}.{tpl.writer.dataset}", top, source, problems)

    files = [
        f
        for folder in get_template_folders()
        for f in sorted((folder.root / "validations").glob("*.yaml"))
    ]
    if rules_file is not None:
        files.append(Path(rules_file))
    for path in files:
        try:
            mapping = load_rules_file(path)
        except Exception as exc:
            problems.append(_config_error(f"{path}: cannot load rules ({exc})"))
            continue
        for key, rules in mapping.items():
            if key not in known:
                problems.append(
                    _config_error(
                        f"{path}: unknown dataset '{key}' (no template produces it)"
                    )
                )
                continue
            _add_rules(index, key, rules, str(path), problems)
    return index, known, problems


def validate_datasets(
    datasets: list[str] | None = None, rules_file: Path | None = None
) -> DoctorReport:
    """Check datasets against their validation rules.

    Args:
        datasets: ``layer.dataset`` names to check. None checks every dataset
            that has rules.
        rules_file: Optional extra rules file.

    Returns:
        A report with rule failures, skipped rules, notes and rule definition
        problems.

    Raises:
        FileNotFoundError: If ``rules_file`` is given but does not exist.
        ValueError: If a requested dataset is not produced by any template.
    """
    if rules_file is not None and not Path(rules_file).is_file():
        raise FileNotFoundError(f"rules file not found: {rules_file}")
    index, known, problems = collect_rules(rules_file)
    if datasets:
        unknown = [d for d in datasets if d not in known]
        if unknown:
            raise ValueError(
                f"unknown dataset(s): {', '.join(unknown)} (no template produces them)"
            )
    issues = list(problems)
    for key in datasets or sorted(index):
        rules = index.get(key)
        if not rules:
            issues.append(
                Issue(
                    category="Data Validation",
                    code="no-rules",
                    severity="info",
                    description=f"no rules for {key}",
                )
            )
            continue
        issues.append(
            Issue(
                category="Data Validation",
                code="validation-run",
                severity="info",
                description=f"{key}: {len(rules)} rule(s) checked",
            )
        )
        issues.extend(run_rules({key: rules}))
    return DoctorReport(issues=issues)
