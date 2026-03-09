"""Dependency resolver for download templates.

When a download template declares a ``dependencies`` block, this module
automatically runs the required upstream templates and injects resolved
argument values before any download begins.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .dependency_graph import TemplateDependencyGraph
from .exceptions import DependencyResolutionError

logger = logging.getLogger(__name__)

MARKER_NAME = ".last_processed"


def _touch_marker(dataset_dir: str) -> None:
    """Write or update a ``.last_processed`` marker in *dataset_dir*.

    If the directory does not exist the call is a no-op.

    Args:
        dataset_dir: Absolute path to the dataset's parquet folder.
    """
    dirpath = Path(dataset_dir)
    if not dirpath.is_dir():
        return
    marker = dirpath / MARKER_NAME
    marker.touch()


def _get_latest_mtime(directory: str) -> float:
    """Return the most recent mtime of any file in *directory* (recursive).

    Args:
        directory: Absolute path to a directory.

    Returns:
        Most recent mtime, or 0.0 if directory is empty or missing.
    """
    dirpath = Path(directory)
    if not dirpath.is_dir():
        return 0.0
    latest = 0.0
    for entry in dirpath.rglob("*"):
        if entry.is_file() and entry.name != MARKER_NAME:
            latest = max(latest, entry.stat().st_mtime)
    return latest


def _is_output_fresh(output_dir: str, input_dirs: list[str]) -> bool:
    """Check whether a dataset's output is fresher than all its inputs.

    Args:
        output_dir: Absolute path to the output dataset folder.
        input_dirs: Absolute paths to input dataset folders.

    Returns:
        ``True`` if the ``.last_processed`` marker in *output_dir* is
        newer than the latest file in every *input_dir*.  ``False``
        when the marker is missing, *output_dir* doesn't exist, or any
        input is newer.
    """
    marker = Path(output_dir) / MARKER_NAME
    if not marker.exists():
        return False
    marker_mtime = marker.stat().st_mtime

    for input_dir in input_dirs:
        input_mtime = _get_latest_mtime(input_dir)
        if input_mtime > marker_mtime:
            return False
    return True


def _dataset_ref_to_id(ref: str) -> str:
    """Convert a dot-separated dataset ref to a slash-separated dataset id.

    Args:
        ref: Dataset reference such as ``"staging.b3-equities-instrument-assets"``.

    Returns:
        Slash-separated id such as ``"staging/b3-equities-instrument-assets"``.
    """
    if "." in ref:
        layer, name = ref.split(".", 1)
        return f"{layer}/{name}"
    return f"input/{ref}"


def _run_sql(datasets: list[str], query: str) -> list:
    """Execute *query* in an in-memory DuckDB connection.

    Registers each dataset in *datasets* as a DuckDB view (using its
    full dotted name as the view name), then executes *query* and
    returns the first column as a list of values.

    Args:
        datasets: List of dataset references (``"layer.dataset-name"``).
        query: SQL query that must return a single column.

    Returns:
        List of values from the first column of the query result.

    Raises:
        RuntimeError: If query execution fails.
    """
    import duckdb

    from brasa.queries import get_dataset

    conn = duckdb.connect(":memory:")
    try:
        for dataset_ref in datasets:
            if "." in dataset_ref:
                layer_name, base_name = dataset_ref.split(".", 1)
                ds = get_dataset(
                    base_name,
                    layer=layer_name,
                    use_template_schema=False,
                    use_catalog_schema=True,
                )
            else:
                ds = get_dataset(dataset_ref)
            conn.register(dataset_ref, ds)

        result_df = conn.execute(query).fetch_df()
        if result_df.empty:
            return []
        return result_df.iloc[:, 0].tolist()
    except Exception as exc:
        raise RuntimeError(f"Dependency SQL query failed: {exc}") from exc
    finally:
        conn.close()


def resolve_dependencies(
    template,
    caller_args: dict,
    _implicit_reports: list | None = None,
) -> dict:
    """Resolve declared dependencies and return injected arg values.

    For each entry in the template's ``dependencies`` block:
    - Skip if the caller already supplies the arg.
    - Validate that all declared datasets exist in the dependency graph.
    - Run the upstream producing template(s) so the dataset is fresh.
    - Execute the SQL query to get the resolved values.
    - If the dep is required and resolution fails, raise
      ``DependencyResolutionError``.

    Args:
        template: A ``MarketDataTemplate`` with an optional
            ``dependencies`` attribute.
        caller_args: Dict of args the caller supplied (from ``**kwargs``
            in ``download_marketdata``).

    Returns:
        Dict mapping arg names to resolved value lists. Only includes
        args that were actually resolved (not skipped).

    Raises:
        DependencyResolutionError: If a required dependency cannot be
            resolved or references an unknown dataset.
    """
    dependencies = getattr(template, "dependencies", None)
    if not dependencies:
        return {}

    # Graph is constructed lazily — only when at least one arg needs resolution
    _graph: TemplateDependencyGraph | None = None
    resolved: dict[str, list] = {}

    for dep_entry in dependencies:
        if not isinstance(dep_entry, dict):
            continue

        for arg_name, dep_config in dep_entry.items():
            # Skip if caller already provides this arg
            if arg_name in caller_args and caller_args[arg_name] is not None:
                logger.debug(
                    "Skipping dependency '%s' for template '%s': "
                    "caller already supplies this arg",
                    arg_name,
                    template.id,
                )
                continue

            # Build the graph on first use
            if _graph is None:
                _graph = TemplateDependencyGraph()
            graph = _graph

            required = dep_config.get("required", True)
            from_block = dep_config.get("from", {})
            dataset_refs = from_block.get("datasets", [])
            query = from_block.get("query", "")

            # Validate all dataset refs exist in the graph (fail-fast)
            for ref in dataset_refs:
                dataset_id = _dataset_ref_to_id(ref)
                producer = graph.get_producer(dataset_id)
                if producer is None:
                    raise DependencyResolutionError(
                        f"Template '{template.id}' dependency '{arg_name}' "
                        f"references unknown dataset '{dataset_id}'. "
                        f"Check the template's dependencies block for typos."
                    )

            # Run upstream producing templates
            _run_upstream_templates(
                template.id,
                arg_name,
                dataset_refs,
                graph,
                required,
                _implicit_reports=_implicit_reports,
            )

            # Run the SQL query to get resolved values
            try:
                values = _run_sql(dataset_refs, query)
            except Exception as exc:
                if required:
                    raise DependencyResolutionError(
                        f"Template '{template.id}' dependency '{arg_name}': "
                        f"SQL query failed: {exc}"
                    ) from exc
                logger.warning(
                    "Optional dependency '%s' for template '%s': "
                    "SQL query failed, running with no args: %s",
                    arg_name,
                    template.id,
                    exc,
                )
                continue

            if not values:
                if required:
                    raise DependencyResolutionError(
                        f"Template '{template.id}' dependency '{arg_name}': "
                        f"SQL query returned no rows."
                    )
                logger.warning(
                    "Optional dependency '%s' for template '%s': "
                    "SQL query returned no rows, running with no args",
                    arg_name,
                    template.id,
                )
                continue

            resolved[arg_name] = values
            logger.debug(
                "Resolved dependency '%s' for template '%s': %d values",
                arg_name,
                template.id,
                len(values),
            )

    return resolved


def _run_upstream_templates(
    template_id: str,
    arg_name: str,
    dataset_refs: list[str],
    graph,
    required: bool,
    _implicit_reports: list | None = None,
) -> None:
    """Run the upstream producing templates for the given dataset refs.

    Args:
        template_id: The template declaring the dependency (for logging).
        arg_name: The arg name being resolved (for error messages).
        dataset_refs: List of dataset references to ensure are up to date.
        graph: The ``TemplateDependencyGraph`` instance.
        required: If ``True``, raise on failure; if ``False``, warn.

    Raises:
        DependencyResolutionError: If a required upstream template fails.
    """
    # Lazy import to avoid circular dependency (api.py imports from this module)
    from .api import process_etl, process_marketdata

    seen_producers: set[str] = set()

    for ref in dataset_refs:
        dataset_id = _dataset_ref_to_id(ref)
        producer = graph.get_producer(dataset_id)
        if producer is None or producer in seen_producers:
            continue
        seen_producers.add(producer)

        template_type = graph.get_template_type(producer)
        logger.info(
            "Running upstream template '%s' (%s) for dependency '%s' of '%s'",
            producer,
            template_type,
            arg_name,
            template_id,
        )

        try:
            if template_type == "etl":
                report = process_etl(producer)
            else:
                report = process_marketdata(producer)

            if _implicit_reports is not None:
                _implicit_reports.append(report)
        except Exception as exc:
            if required:
                raise DependencyResolutionError(
                    f"Template '{template_id}' dependency '{arg_name}': "
                    f"upstream template '{producer}' failed: {exc}"
                ) from exc
            logger.warning(
                "Optional dependency '%s' for template '%s': "
                "upstream template '%s' failed (will try stale data): %s",
                arg_name,
                template_id,
                producer,
                exc,
            )
            return

        if not report.success:
            if required:
                raise DependencyResolutionError(
                    f"Template '{template_id}' dependency '{arg_name}': "
                    f"upstream template '{producer}' reported failures."
                )
            logger.warning(
                "Optional dependency '%s' for template '%s': "
                "upstream template '%s' reported failures (will try stale data)",
                arg_name,
                template_id,
                producer,
            )
