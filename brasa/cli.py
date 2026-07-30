import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

from . import (
    download_marketdata,
    import_marketdata,
    process_etl,
    process_marketdata,
    retrieve_template,
)
from .engine import CacheManager, Verbosity, sync_catalog_from_disk
from .engine.exceptions import BrasaNotConfiguredError
from .queries import BrasaDB, describe_dataset, get_dataset, list_datasets
from .util import parse_arg_value


def add_verbosity_args(parser: argparse.ArgumentParser) -> None:
    """Add verbosity and report arguments to a parser."""
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="verbose output: show each task on its own line",
    )
    verbosity_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="quiet output: only show summary if there are errors",
    )
    parser.add_argument(
        "--report",
        metavar="FILE",
        help="save report to file (JSON if .json extension, otherwise TXT)",
    )


def get_verbosity(args: argparse.Namespace) -> Verbosity:
    """Get verbosity level from parsed arguments."""
    if getattr(args, "verbose", False):
        return Verbosity.VERBOSE
    elif getattr(args, "quiet", False):
        return Verbosity.QUIET
    return Verbosity.NORMAL


def _format_dataframe_for_display(
    df: pd.DataFrame,
    width: int | None = None,
    max_colwidth: int = 50,
    columns: list[str] | None = None,
    wrap: bool = False,
) -> str:
    """Format a DataFrame for terminal display.

    Args:
        df: DataFrame to format.
        width: Terminal width. If None, auto-detects.
        max_colwidth: Maximum width for each column content.
        columns: List of columns to display. If None, displays all.
        wrap: If True, wraps columns across multiple rows.

    Returns:
        Formatted string representation of the DataFrame.
    """
    terminal_width = width if width is not None else shutil.get_terminal_size().columns

    # Filter columns if specified
    if columns:
        available_cols = [c for c in columns if c in df.columns]
        if available_cols:
            df = df[available_cols]

    if wrap:
        # Wrap columns across multiple rows
        return df.to_string(
            line_width=terminal_width,
            max_colwidth=max_colwidth,
        )
    else:
        # No wrapping: each row on single line, truncated to terminal width
        output = df.to_string(line_width=None, max_colwidth=max_colwidth)
        lines = output.split("\n")
        truncated_lines = []
        for line in lines:
            if len(line) > terminal_width:
                truncated_lines.append(line[: terminal_width - 3] + "...")
            else:
                truncated_lines.append(line)
        return "\n".join(truncated_lines)


def _infer_output_format(path: str) -> str | None:
    """Infer an output format from a file path's extension.

    Returns:
        One of "csv", "json", "parquet", "orc", "xls", or None if the
        extension isn't recognized.
    """
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".json"):
        return "json"
    if path.endswith(".parquet"):
        return "parquet"
    if path.endswith(".orc"):
        return "orc"
    if path.endswith(".xls") or path.endswith(".xlsx"):
        return "xls"
    return None


def _parse_layer_dataset(value: str) -> tuple[str, str]:
    """Parse a 'layer.dataset' CLI argument.

    Raises:
        ValueError: If value isn't in valid layer.dataset format.
    """
    parts = value.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "Invalid dataset format. Use layer.dataset (e.g., input.b3-cotahist)"
        )
    return parts[0], parts[1]


parser = argparse.ArgumentParser(
    description="Brasa CLI for downloading and processing market data",
)

subparsers = parser.add_subparsers(dest="command", title="Commands")

parser_init = subparsers.add_parser(
    "init", help="initialize brasa: choose the data directory and persist it"
)
parser_init.add_argument(
    "--data-path",
    help="data directory to use (skips the prompt)",
)
parser_init.add_argument(
    "-y",
    "--yes",
    action="store_true",
    help="accept the suggested default without prompting",
)

parser_download = subparsers.add_parser("download", help="download market data")
parser_download.add_argument(
    "--arg",
    action="append",
    metavar="KEY=VALUE",
    help=(
        "download argument as KEY=VALUE. Repeatable. "
        "Prefixes: @=date (@2026-01, @2026-01-01:2026-01-31, @2026-01~ANBIMA), "
        "$=symbols ($index, $company). "
        "Commas create lists (IBOV,BOVA11). Bare integers auto-convert."
    ),
)
parser_download.add_argument(
    "--calendar",
    help="specify calendar to be used for creating date range (default: B3)",
    default=None,
    choices=["B3", "ANBIMA"],
)
parser_download.add_argument(
    "--plan",
    metavar="FILE",
    help="path to a download plan YAML file (mutually exclusive with template names)",
)
parser_download.add_argument(
    "template",
    nargs="*",
    help="template names (required unless --plan is used)",
)
parser_download.add_argument(
    "--force",
    action="store_true",
    help="force re-download even if data already exists in cache",
)
parser_download.add_argument(
    "--update",
    action="store_true",
    help="smart update mode: auto-detect strategy and resolve incremental download kwargs",
)
parser_download.add_argument(
    "--since",
    metavar="YYYY-MM-DD",
    help="explicit start date for incremental strategies (overrides cache query)",
)
add_verbosity_args(parser_download)

parser_import = subparsers.add_parser(
    "import", help="import local files into a template"
)
parser_import.add_argument("template", nargs="+", help="template names")
parser_import.add_argument(
    "--path",
    metavar="PATH",
    help="local file path or pattern (overrides the template path:)",
)
parser_import.add_argument(
    "--arg",
    action="append",
    metavar="KEY=VALUE",
    help=(
        "import argument as KEY=VALUE. Repeatable. Same DSL as download: "
        "@=date (@2026-01-01:2026-01-31), $=symbols. Commas create lists."
    ),
)
parser_import.add_argument(
    "--calendar",
    default="B3",
    choices=["B3", "ANBIMA"],
    help="calendar for date range expansion",
)
parser_import.add_argument(
    "--force",
    action="store_true",
    help="force re-import even if data already exists in cache",
)
add_verbosity_args(parser_import)

parser_process = subparsers.add_parser(
    "process", help="process market data - transform raw data to parquet files"
)
parser_process.add_argument("template", nargs="+", help="template names")
parser_process.add_argument(
    "--reprocess",
    action="store_true",
    help="reprocess all files, even if already processed",
)
parser_process.add_argument(
    "--show-skipped",
    action="store_true",
    help="show skipped (cached) entries in progress display; default: hidden",
)
add_verbosity_args(parser_process)

parser_create_views = subparsers.add_parser(
    "create-views", help="create all views in brasa database"
)
parser_create_views.add_argument(
    "--layer",
    help="specify layer to create views for (raw, input, staging, curated)",
    choices=["raw", "input", "staging", "curated"],
)

parser_create_view = subparsers.add_parser(
    "create-view", help="create specific view in brasa database"
)
parser_create_view.add_argument("template", nargs="+", help="template names")

parser_list_tables = subparsers.add_parser(
    "list-tables", help="list available layer.dataset tables"
)
parser_list_tables.add_argument(
    "--layer",
    help="filter by specific layer",
    choices=["raw", "input", "staging", "curated"],
)
parser_list_tables.add_argument(
    "-v", "--verbose", action="store_true", help="show additional information"
)

parser_query = subparsers.add_parser(
    "query", help="execute read-only SQL queries on brasa database"
)
parser_query.add_argument("sql_query", help="SQL query to be executed")
parser_query.add_argument(
    "-o",
    "--output",
    help="output format (display, csv, json, parquet, xlsx)",
    default="display",
)
parser_query.add_argument(
    "--list-tables", action="store_true", help="list available tables and exit"
)
parser_query.add_argument(
    "-v", "--verbose", action="store_true", help="show query execution details"
)

parser_head = subparsers.add_parser("head", help="show first N rows of a dataset")
parser_head.add_argument(
    "dataset",
    help="dataset name in format layer.dataset (e.g., input.b3-cotahist, staging.b3-cotahist)",
)
parser_head.add_argument(
    "-n",
    "--lines",
    type=int,
    default=10,
    help="number of rows to display (default: 10)",
)
parser_head.add_argument(
    "-o",
    "--output",
    default="display",
    help="output format: display (default) or file path (.csv, .json, .parquet, .xlsx)",
)
parser_head.add_argument(
    "-w",
    "--width",
    type=int,
    default=None,
    help="terminal width override (default: auto-detect)",
)
parser_head.add_argument(
    "--max-colwidth",
    type=int,
    default=50,
    help="maximum width for column content (default: 50)",
)
parser_head.add_argument(
    "-c",
    "--columns",
    nargs="+",
    help="specific columns to display (space-separated)",
)
parser_head.add_argument(
    "--wrap",
    action="store_true",
    help="wrap columns across multiple rows instead of truncating",
)

# Dataset catalog commands
parser_list_datasets = subparsers.add_parser(
    "list-datasets", help="list all registered datasets in the catalog"
)
parser_list_datasets.add_argument(
    "--layer",
    choices=["input", "staging", "curated"],
    help="filter by data layer",
)
parser_list_datasets.add_argument(
    "--format",
    choices=["table", "json"],
    default="table",
    help="output format (default: table)",
)

parser_describe_dataset = subparsers.add_parser(
    "describe-dataset", help="show detailed information about a dataset"
)
parser_describe_dataset.add_argument(
    "dataset",
    help="dataset name in format layer.dataset (e.g., input.b3-cotahist)",
)
parser_describe_dataset.add_argument(
    "--compare-template",
    action="store_true",
    help="compare catalog schema with template schema",
)
parser_describe_dataset.add_argument(
    "--format",
    choices=["text", "json"],
    default="text",
    help="output format (default: text)",
)

parser_sync_catalog = subparsers.add_parser(
    "sync-catalog",
    help="scan db/ folder and register untracked datasets in the catalog",
)
parser_sync_catalog.add_argument(
    "--layer",
    choices=["input", "staging", "curated"],
    help="filter by data layer",
)
parser_sync_catalog.add_argument(
    "--dry-run",
    action="store_true",
    help="preview changes without making them",
)
parser_sync_catalog.add_argument(
    "--force",
    action="store_true",
    help="overwrite existing catalog entries",
)
parser_sync_catalog.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    help="show detailed output",
)
parser_sync_catalog.add_argument(
    "--format",
    choices=["text", "json"],
    default="text",
    help="output format (default: text)",
)

# Dependency graph commands
parser_deps = subparsers.add_parser(
    "deps", help="show upstream dependencies for a template"
)
parser_deps.add_argument("template", help="template name")

parser_plan = subparsers.add_parser("plan", help="show execution plan for a template")
parser_plan.add_argument("template", help="template name")
parser_plan.add_argument(
    "--force",
    action="store_true",
    help="mark all ancestors for execution regardless of staleness",
)

parser_run = subparsers.add_parser(
    "run", help="execute a template with automatic dependency resolution"
)
parser_run.add_argument("template", help="template name")
parser_run.add_argument(
    "--force",
    action="store_true",
    help="re-execute all upstream templates regardless of staleness",
)
parser_run.add_argument(
    "--dry-run",
    action="store_true",
    help="show execution plan without running anything",
)
add_verbosity_args(parser_run)

parser_run_all = subparsers.add_parser(
    "run-all",
    help="converge the whole pipeline: process/ETL everything stale",
)
parser_run_all.add_argument(
    "--dry-run",
    action="store_true",
    help="show the predicted execution plan without running anything",
)
add_verbosity_args(parser_run_all)

parser_list_unprocessed = subparsers.add_parser(
    "list-unprocessed",
    help="list download templates with unprocessed (downloaded but not parsed) files",
)
parser_list_unprocessed.add_argument(
    "--format",
    choices=["table", "json"],
    default="table",
    help="output format (default: table)",
)

parser_list_templates = subparsers.add_parser(
    "list-templates",
    help="list available templates and their source (bundled or user path)",
)

parser_graph = subparsers.add_parser(
    "graph", help="export or render the dependency graph"
)
parser_graph.add_argument(
    "--format",
    metavar="FORMAT",
    choices=["dot", "ascii", "png", "svg", "pdf"],
    default="dot",
    help="output format: dot (default), ascii, png, svg, pdf",
)
parser_graph.add_argument(
    "--output",
    metavar="FILE",
    help="write output to file (default: stdout for text formats)",
)
parser_graph.add_argument(
    "--template",
    metavar="NAME",
    help="show only the subgraph for a specific template",
)


parser_map = subparsers.add_parser(
    "map",
    help="show a global staleness report across all templates",
)
parser_map.add_argument(
    "--format",
    choices=["flat", "grouped", "tree"],
    default="flat",
    help="output layout (default: flat)",
)
parser_map.add_argument(
    "--all",
    action="store_true",
    help="include up-to-date templates (status 'ok')",
)
parser_map.add_argument(
    "--reverse",
    action="store_true",
    help="for --format tree: root at leaves and branch upward to sources",
)
parser_map.add_argument(
    "--no-color",
    action="store_true",
    help="disable ANSI colors",
)


parser_doctor = subparsers.add_parser(
    "doctor",
    help="diagnose cache health: orphan files, missing data, schema drift, date gaps",
)
parser_doctor.add_argument(
    "--fix",
    action="store_true",
    help="apply all auto-fixable issues",
)
parser_doctor.add_argument(
    "--yes",
    action="store_true",
    help="skip confirmation prompt when using --fix",
)
parser_doctor.add_argument(
    "--category",
    nargs="+",
    metavar="CATEGORY",
    choices=["raw", "db", "meta", "templates", "gaps", "validations", "downloads"],
    help=(
        "run only specific categories: raw, db, meta, templates, gaps, "
        "validations, downloads"
    ),
)
parser_doctor.add_argument(
    "--template",
    nargs="+",
    metavar="TEMPLATE",
    help=(
        "restrict the date-gaps, stale-etl, missing-etl-source and downloads "
        "checks to specific templates"
    ),
)
parser_doctor.add_argument(
    "--calendar",
    metavar="NAME",
    default="B3",
    help="business calendar for the gaps and downloads categories (default: B3)",
)


def _last_value(value: str) -> int:
    """Parse the doctor --last option: a number of days, -1, or 'all'."""
    if value == "all":
        return -1
    n = int(value)  # argparse turns ValueError into a clean usage error
    if n < -1:
        raise argparse.ArgumentTypeError("must be a number of days, -1, or 'all'")
    return n


parser_doctor.add_argument(
    "--last",
    type=_last_value,
    default=30,
    metavar="DAYS",
    help=(
        "for the gaps and downloads categories, look back this many days; "
        "'all' or -1 reviews the full history (default: 30)"
    ),
)
parser_doctor.add_argument(
    "--validations-file",
    metavar="FILE",
    help="path to a validations YAML file (required for the 'validations' category)",
)

parser_cache = subparsers.add_parser(
    "cache",
    help="cache maintenance operations",
)
cache_subparsers = parser_cache.add_subparsers(
    dest="cache_command", title="Cache commands"
)

parser_cache_drop = cache_subparsers.add_parser(
    "drop",
    help="drop a cache entry by meta id (raw files + metadata + trials)",
)
parser_cache_drop.add_argument(
    "meta_id",
    metavar="META_ID",
    help="cache entry id (see meta.db cache_metadata.id)",
)
parser_cache_drop.add_argument(
    "--yes",
    action="store_true",
    help="skip confirmation prompt",
)


def _format_datasets_table(datasets) -> str:
    """Format datasets as a table string."""
    if not datasets:
        return "No datasets found."

    # Calculate column widths
    headers = ["Layer", "Dataset", "Columns", "Partitioning", "Source Template"]
    rows = []
    for d in datasets:
        rows.append(
            [
                d.layer,
                d.dataset_name,
                str(len(d.schema)),
                ", ".join(d.partitioning) if d.partitioning else "-",
                d.source_template or "-",
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    # Build table
    lines = []
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    lines.append(header_line)
    lines.append("-" * len(header_line))
    for row in rows:
        lines.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))

    return "\n".join(lines)


def _format_dataset_info(info: dict) -> str:
    """Format dataset info as text."""
    lines = [
        f"Dataset: {info['layer']}.{info['dataset_name']}",
        f"ID: {info['id']}",
        f"Source Template: {info['source_template'] or 'N/A'}",
        f"Created: {info['created_at']}",
        f"Updated: {info['updated_at']}",
        f"Partitioning: {', '.join(info['partitioning']) if info['partitioning'] else 'None'}",
        "",
        f"Schema ({len(info['schema'])} columns):",
    ]

    for field in info["schema"]:
        nullable = "nullable" if field["nullable"] else "not null"
        lines.append(f"  {field['name']}: {field['type']} ({nullable})")

    if info.get("schema_differences"):
        lines.append("")
        lines.append("Schema differences from template:")
        for diff in info["schema_differences"]:
            if diff["issue"] == "type_mismatch":
                lines.append(
                    f"  {diff['field']}: catalog={diff['catalog_type']}, "
                    f"template={diff['template_type']}"
                )
            elif diff["issue"] == "in_catalog_only":
                lines.append(
                    f"  {diff['field']}: only in catalog ({diff['catalog_type']})"
                )
            elif diff["issue"] == "in_template_only":
                lines.append(
                    f"  {diff['field']}: only in template ({diff['template_type']})"
                )

    return "\n".join(lines)


def _format_migration_report(report, verbose: bool = False) -> str:
    """Format migration report as text."""
    lines = []

    if report.would_register:
        lines.append("Would register:")
        for dataset_id, schema, partitioning in report.would_register:
            layer, name = dataset_id.split("/", 1)
            lines.append(f"  [{layer}] {name}")
            if verbose:
                lines.append(f"    Schema: {len(schema)} columns")
                if partitioning:
                    lines.append(f"    Partitioning: {partitioning}")
        lines.append("")

    if report.registered:
        lines.append("Registered:")
        for dataset_id in report.registered:
            lines.append(f"  {dataset_id}")
        lines.append("")

    if report.skipped:
        lines.append("Skipped:")
        for dataset_id, reason in report.skipped:
            lines.append(f"  {dataset_id}: {reason}")
        lines.append("")

    if report.errors:
        lines.append("Errors:")
        for dataset_id, error in report.errors:
            lines.append(f"  {dataset_id}: {error}")
        lines.append("")

    if report.warnings:
        lines.append("Warnings:")
        for warning in report.warnings:
            lines.append(f"  {warning}")
        lines.append("")

    lines.append(report.summary())
    return "\n".join(lines)


def _print_doctor_report(report) -> None:
    """Print a formatted doctor report to stdout."""
    SEV_PREFIX = {"error": "  \u2717", "warning": "  \u26a0", "info": "  i"}

    print("\nBrasa Doctor")
    print("════════════")

    if not report.issues:
        print("\n  ✓  No issues found.")
        print(f"\n{'─' * 36}")
        print("Summary: no issues")
        return

    # Group by category
    categories: dict[str, list] = {}
    for issue in report.issues:
        categories.setdefault(issue.category, []).append(issue)

    for category, issues in categories.items():
        print(f"\n{category}")
        for issue in issues:
            icon = SEV_PREFIX.get(issue.severity, "  ?")
            print(f"{icon}  {issue.description}")
            for detail in issue.details[:10]:
                print(f"       {detail}")
            if len(issue.details) > 10:
                print(f"       … and {len(issue.details) - 10} more")
            if issue.fixable:
                print("     → fixable: run with --fix to apply")

    print(f"\n{'─' * 36}")
    summary = report.summary()
    print(f"Summary: {summary}")
    if report.fixable():
        print("Run `brasa doctor --fix` to apply auto-fixes.")


def _generate_dot(graph, template_id: str | None = None) -> str:
    """Generate a DOT representation of the dependency graph.

    Args:
        graph: A ``TemplateDependencyGraph`` instance.
        template_id: If given, restrict to the subgraph of this template
            and its ancestors.

    Returns:
        A string in DOT format.
    """
    if template_id is not None:
        ancestors = graph.get_ancestors(template_id)
        nodes = ancestors | {template_id}
    else:
        nodes = set(graph.template_ids)

    lines = ["digraph dependencies {", "  rankdir=LR;", "  node [shape=box];"]

    for tid in sorted(nodes):
        ttype = graph.get_template_type(tid)
        if ttype == "download":
            lines.append(f'  "{tid}" [style=filled, fillcolor=lightblue];')
        else:
            lines.append(f'  "{tid}" [style=filled, fillcolor=lightyellow];')

    for tid in sorted(nodes):
        for upstream in graph.get_upstream(tid):
            if upstream in nodes:
                lines.append(f'  "{upstream}" -> "{tid}";')

    lines.append("}")
    return "\n".join(lines)


def _render_ascii(graph, template_id: str | None = None) -> str:
    """Render the dependency graph as an ASCII tree.

    Args:
        graph: A ``TemplateDependencyGraph`` instance.
        template_id: If given, restrict to the subgraph of this template
            and its ancestors.

    Returns:
        A string with the ASCII tree representation.
    """
    if template_id is not None:
        ancestors = graph.get_ancestors(template_id)
        nodes = ancestors | {template_id}
    else:
        nodes = set(graph.template_ids)

    def _tree_lines(node: str, prefix: str, visited: set) -> list[str]:
        ttype = graph.get_template_type(node)
        tag = "[D]" if ttype == "download" else "[E]"
        head = f"{tag} {node}"
        if node in visited:
            return [head + "  (already shown above)"]
        visited = visited | {node}
        upstream = sorted(u for u in graph.get_upstream(node) if u in nodes)
        lines = [head]
        for i, dep in enumerate(upstream):
            is_last = i == len(upstream) - 1
            connector = "└── " if is_last else "├── "
            extension = "    " if is_last else "│   "
            child_lines = _tree_lines(dep, prefix + extension, visited)
            lines.append(prefix + connector + child_lines[0])
            for cl in child_lines[1:]:
                lines.append(cl)
        return lines

    if template_id is not None:
        all_lines = _tree_lines(template_id, "", set())
    else:
        roots = sorted(
            t for t in nodes if not any(d in nodes for d in graph.get_downstream(t))
        )
        all_lines = []
        for root in roots:
            all_lines.extend(_tree_lines(root, "", set()))
            all_lines.append("")

    legend = ["[D] = download   [E] = etl/processing", ""]
    return "\n".join(legend + all_lines)


def _parse_download_args(raw_args: list[str] | None, calendar: str) -> dict:
    """Parse --arg KEY=VALUE pairs into a kwargs dict.

    Raises:
        ValueError: If an item is not in KEY=VALUE form.
    """
    if not raw_args:
        return {}
    kwargs = {}
    for item in raw_args:
        if "=" not in item:
            raise ValueError(f"invalid --arg format: {item!r} (expected KEY=VALUE)")
        key, value = item.split("=", 1)
        kwargs[key] = parse_arg_value(value, default_calendar=calendar)
    return kwargs


def _cmd_init(args) -> None:
    """Handle `brasa init`: choose, create, and persist the data directory.

    Args:
        args: Parsed CLI namespace with ``data_path`` and ``yes`` attributes.
    """
    from .engine.config import (
        config_file_path,
        default_data_path,
        load_config,
        save_data_path,
    )

    current = load_config().get("data_path")
    suggested = args.data_path or current or default_data_path()
    interactive = (
        args.data_path is None
        and not args.yes
        and sys.stdin is not None
        and sys.stdin.isatty()
    )
    if interactive:
        answer = input(f"Data directory [{suggested}]: ").strip()
        chosen = answer or suggested
    else:
        chosen = suggested

    chosen_path = Path(chosen).expanduser().resolve()
    chosen_path.mkdir(parents=True, exist_ok=True)
    save_data_path(str(chosen_path))

    print(f"Brasa home ready at {chosen_path}")
    print(f"Configuration saved to {config_file_path()}")
    env_override = os.environ.get("BRASA_DATA_PATH")
    if env_override:
        print(
            f"Note: BRASA_DATA_PATH is set ({env_override}) "
            "and takes precedence over the config file."
        )


def main() -> None:
    args = parser.parse_args()
    if args.command == "init":
        _cmd_init(args)
        return
    try:
        _run_command(args)
    except BrasaNotConfiguredError:
        print("Error: brasa is not configured yet.", file=sys.stderr)
        print(
            "Run `brasa init` to set up your data directory, or set BRASA_DATA_PATH.",
            file=sys.stderr,
        )
        sys.exit(1)


def _run_command(args) -> None:  # noqa: PLR0912, PLR0915
    if args.command == "download":
        plan_file = getattr(args, "plan", None)
        templates = list(args.template or [])
        verbosity = get_verbosity(args)
        report_file = getattr(args, "report", None)
        smart_update = getattr(args, "update", False)
        since = getattr(args, "since", None)
        calendar = args.calendar or "B3"

        if plan_file and templates:
            print(
                "Error: --plan and template names are mutually exclusive",
                file=sys.stderr,
            )
            sys.exit(1)
        if not plan_file and not templates:
            print(
                "Error: either --plan or at least one template name is required",
                file=sys.stderr,
            )
            sys.exit(1)

        # Parse --arg with the effective calendar (both paths use this).
        try:
            download_kwargs = _parse_download_args(args.arg, calendar)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)

        # Mutual exclusivity applies to BOTH paths: smart update auto-resolves
        # dates, so an explicit refdate contradicts it.
        if smart_update and "refdate" in download_kwargs:
            print(
                "Error: --update and --arg refdate=... are mutually exclusive",
                file=sys.stderr,
            )
            sys.exit(1)

        if plan_file:
            from .engine.download_plan import DownloadPlan, execute_download_plan

            plan = DownloadPlan.from_file(plan_file)
            errors = plan.validate()
            if errors:
                for err in errors:
                    print(f"Error: {err}", file=sys.stderr)
                sys.exit(1)
            refdate_override = download_kwargs.pop("refdate", None)
            try:
                execute_download_plan(
                    plan,
                    refdate_override=refdate_override,
                    force_override=args.force,
                    smart_update_override=(True if args.update else None),
                    since=since,
                    extra_args=download_kwargs,
                    calendar_override=args.calendar,
                    verbosity=verbosity,
                    report_file=report_file,
                )
            except ValueError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                sys.exit(1)
        else:
            # Direct-template path.
            if since and not smart_update:
                print(
                    "Error: --since requires --update",
                    file=sys.stderr,
                )
                sys.exit(1)

            if verbosity != Verbosity.QUIET:
                print(
                    "Status legend: .(passed) F(failed) E(error) "
                    "S(skipped) D(duplicated) I(invalid) C(corrupted) N(no-data) "
                    "r(retry)"
                )
            for template in templates:
                download_marketdata(
                    template,
                    force=args.force,
                    smart_update=smart_update,
                    calendar=calendar,
                    verbosity=verbosity,
                    report_file=report_file,
                    **({"since": since} if since else {}),
                    **download_kwargs,
                )
    elif args.command == "import":
        verbosity = get_verbosity(args)
        report_file = getattr(args, "report", None)
        templates = list(args.template or [])
        try:
            download_kwargs = _parse_download_args(args.arg, args.calendar)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        if args.path is not None:
            download_kwargs["path"] = args.path
        for template in templates:
            import_marketdata(
                template,
                force=args.force,
                verbosity=verbosity,
                report_file=report_file,
                **download_kwargs,
            )
    elif args.command == "process":
        verbosity = get_verbosity(args)
        report_file = getattr(args, "report", None)
        for template in args.template:
            _template = retrieve_template(template)
            if _template.is_etl:
                process_etl(
                    template,
                    verbosity=verbosity,
                    report_file=report_file,
                )
            else:
                process_marketdata(
                    template,
                    reprocess=args.reprocess,
                    verbosity=verbosity,
                    report_file=report_file,
                    show_skipped=args.show_skipped,
                )
    elif args.command == "create-views":
        layers = [args.layer] if hasattr(args, "layer") and args.layer else None
        results = BrasaDB.create_all_views(layers)
        if results:
            created = sum(1 for v in results.values() if v)
            total = len(results)
            print(f"\nSuccessfully created {created}/{total} views")
        else:
            print("No views were created")
    elif args.command == "create-view":
        for template in args.template:
            BrasaDB.create_view(template)
            print(f"View created: {template}")
    elif args.command == "list-tables":
        tables = BrasaDB.list_tables()
        if not tables:
            print("No tables found. Create views first with: brasa create-views")
            sys.exit(0)

        # Filter by layer if specified
        layer_filter = getattr(args, "layer", None)
        if layer_filter:
            tables = [t for t in tables if t.startswith(layer_filter + ".")]

        if getattr(args, "verbose", False):
            print(f"{'Table Name':<50} | {'Rows':>12}")
            print("-" * 65)
            for table in tables:
                try:
                    result = BrasaDB.query(f'SELECT COUNT(*) as cnt FROM "{table}"')
                    count = result.df().iloc[0, 0]
                    print(f"{table:<50} | {count:>12,}")
                except Exception as e:
                    print(f"{table:<50} | Error: {str(e)[:20]}")
        else:
            for table in tables:
                print(table)

    elif args.command == "query":
        # Check if user wants to list tables
        if getattr(args, "list_tables", False):
            tables = BrasaDB.list_tables()
            if not tables:
                print("No tables found. Create views first with: brasa create-views")
                sys.exit(0)
            print("Available tables:")
            for table in tables:
                print(f"  {table}")
            sys.exit(0)

        # Create views if they don't exist
        existing_tables = BrasaDB.list_tables()
        if not existing_tables:
            print("Creating views... (first time setup)")
            BrasaDB.create_all_views()

        # Execute query
        q = BrasaDB.query(args.sql_query)
        output = args.output

        if getattr(args, "verbose", False):
            # Show query execution info (could add EXPLAIN here)
            try:
                explain = BrasaDB.query(f"EXPLAIN {args.sql_query}")
                print("Query Plan:")
                print(explain)
                print("\n" + "=" * 60 + "\n")
            except Exception:
                pass  # If EXPLAIN fails, just skip it

        if output == "display":
            print(q)
        else:
            fmt = _infer_output_format(output)
            if fmt == "csv":
                q.df().to_csv(output, sep=",", encoding="utf-8", index=False)
                print(f"Results saved to {output}")
            elif fmt == "json":
                q.df().to_json(output, index=False)
                print(f"Results saved to {output}")
            elif fmt == "parquet":
                q.df().to_parquet(output, index=False)
                print(f"Results saved to {output}")
            elif fmt == "orc":
                q.df().to_orc(output, index=False)
                print(f"Results saved to {output}")
            elif fmt == "xls":
                q.df().to_excel(output, index=False)
                print(f"Results saved to {output}")
    elif args.command == "head":
        try:
            layer, dataset_name = _parse_layer_dataset(args.dataset)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        try:
            ds = get_dataset(dataset_name, layer=layer)
            df = ds.head(args.lines).to_pandas()
        except FileNotFoundError:
            print(f"Error: Dataset '{layer}.{dataset_name}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error: Failed to load dataset '{layer}.{dataset_name}': {e}")
            sys.exit(1)

        output = args.output
        if output == "display":
            print(
                _format_dataframe_for_display(
                    df,
                    width=args.width,
                    max_colwidth=args.max_colwidth,
                    columns=args.columns,
                    wrap=args.wrap,
                )
            )
        else:
            fmt = _infer_output_format(output)
            if fmt == "csv":
                df.to_csv(output, sep=",", encoding="utf-8", index=False)
            elif fmt == "json":
                df.to_json(output, orient="records", indent=2)
            elif fmt == "parquet":
                df.to_parquet(output, index=False)
            elif fmt == "xls":
                df.to_excel(output, index=False)
            else:
                print(f"Error: Unsupported output format: {output}")
                sys.exit(1)

    elif args.command == "list-datasets":
        datasets = list_datasets(layer=args.layer)
        if args.format == "json":
            output = [
                {
                    "id": d.id,
                    "layer": d.layer,
                    "dataset_name": d.dataset_name,
                    "columns": len(d.schema),
                    "partitioning": d.partitioning,
                    "source_template": d.source_template,
                    "created_at": d.created_at.isoformat(),
                    "updated_at": d.updated_at.isoformat(),
                }
                for d in datasets
            ]
            print(json.dumps(output, indent=2))
        else:
            print(_format_datasets_table(datasets))

    elif args.command == "describe-dataset":
        try:
            layer, dataset_name = _parse_layer_dataset(args.dataset)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        try:
            info = describe_dataset(
                layer, dataset_name, compare_template=args.compare_template
            )
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        if args.format == "json":
            print(json.dumps(info, indent=2))
        else:
            print(_format_dataset_info(info))

    elif args.command == "sync-catalog":
        print("Scanning db/ folder for datasets...")
        report = sync_catalog_from_disk(
            layer=args.layer,
            dry_run=args.dry_run,
            force=args.force,
        )

        if args.format == "json":
            print(json.dumps(report.to_dict(), indent=2))
        else:
            print(_format_migration_report(report, verbose=args.verbose))

        if args.dry_run:
            print("\nRun without --dry-run to apply changes.")

    elif args.command == "deps":
        from .engine.dependency_graph import TemplateDependencyGraph

        graph = TemplateDependencyGraph()
        template = args.template
        if template not in graph:
            print(f"Error: Template '{template}' not found in dependency graph.")
            sys.exit(1)

        upstream = graph.get_upstream(template)
        ancestors = graph.get_ancestors(template)
        downstream = graph.get_downstream(template)
        ttype = graph.get_template_type(template)
        outputs = graph.get_outputs(template)

        print(f"Template: {template} ({ttype})")
        print(f"Outputs: {', '.join(o.dataset_id for o in outputs)}")
        print()
        if upstream:
            print(f"Direct upstream ({len(upstream)}):")
            for u in upstream:
                print(f"  {u} ({graph.get_template_type(u)})")
        else:
            print("Direct upstream: (none)")
        print()
        if ancestors:
            print(f"All ancestors ({len(ancestors)}):")
            for a in sorted(ancestors):
                print(f"  {a} ({graph.get_template_type(a)})")
        else:
            print("All ancestors: (none)")
        print()
        if downstream:
            print(f"Direct downstream ({len(downstream)}):")
            for d in downstream:
                print(f"  {d} ({graph.get_template_type(d)})")
        else:
            print("Direct downstream: (none)")

    elif args.command == "plan":
        from .engine.dependency_graph import TemplateDependencyGraph

        graph = TemplateDependencyGraph()
        template = args.template
        if template not in graph:
            print(f"Error: Template '{template}' not found in dependency graph.")
            sys.exit(1)

        plan = graph.get_execution_plan(template, force=args.force)
        print(plan)

    elif args.command == "run":
        from .engine.orchestrator import PipelineOrchestrator

        verbosity = get_verbosity(args)
        orchestrator = PipelineOrchestrator()
        report = orchestrator.execute(
            args.template,
            force=args.force,
            dry_run=args.dry_run,
            verbosity=verbosity,
        )
        print(report.summary())

        report_file = getattr(args, "report", None)
        if report_file:
            # Save the target template's report if available
            target_report = report.step_reports.get(args.template)
            if target_report:
                report_path = Path(report_file)
                file_format = "json" if report_path.suffix == ".json" else "txt"
                target_report.save_report(report_path, format=file_format)

        if not report.success:
            sys.exit(1)

    elif args.command == "run-all":
        from .engine.orchestrator import PipelineOrchestrator

        verbosity = get_verbosity(args)
        orchestrator = PipelineOrchestrator()
        report = orchestrator.execute_all(
            dry_run=args.dry_run,
            verbosity=verbosity,
        )
        print(report.summary())

        if not report.success:
            sys.exit(1)

    elif args.command == "list-unprocessed":
        manager = CacheManager()
        results = manager.get_templates_with_unprocessed_downloads()
        if args.format == "json":
            print(json.dumps(results, indent=2))
        elif not results:
            print("No templates with unprocessed downloads.")
        else:
            name_width = max(len(r["template"]) for r in results)
            name_width = max(name_width, len("Template"))
            print(f"{'Template':<{name_width}}  {'Unprocessed':>11}")
            print("-" * (name_width + 13))
            for r in results:
                print(f"{r['template']:<{name_width}}  {r['count']:>11}")
            print("-" * (name_width + 13))
            total = sum(r["count"] for r in results)
            print(f"{'Total':<{name_width}}  {total:>11}")

    elif args.command == "list-templates":
        from .engine.resources import package_path
        from .engine.template import list_template_sources

        bundled_root = package_path("templates").resolve()
        entries = list_template_sources()
        if not entries:
            print("No templates found.")
        else:
            name_width = max(len("NAME"), *(len(e.name) for e in entries))
            print(f"{'NAME':<{name_width}}  SOURCE")
            for e in entries:
                if e.source.resolve() == bundled_root:
                    source = "bundled"
                else:
                    source = f"{e.source} (user)"
                if e.shadows:
                    source += " *shadows bundled"
                print(f"{e.name:<{name_width}}  {source}")

    elif args.command == "graph":
        from .engine.dependency_graph import TemplateDependencyGraph

        graph = TemplateDependencyGraph()
        template = getattr(args, "template", None)
        if template and template not in graph:
            print(f"Error: Template '{template}' not found in dependency graph.")
            sys.exit(1)

        fmt = getattr(args, "format", "dot")
        output_file = getattr(args, "output", None)
        dot = _generate_dot(graph, template_id=template)

        if fmt == "ascii":
            text = _render_ascii(graph, template_id=template)
            if output_file:
                Path(output_file).write_text(text + "\n")
                print(f"Graph written to {output_file}")
            else:
                print(text)
        elif fmt == "dot":
            if output_file:
                with Path(output_file).open("w") as f:
                    f.write(dot)
                    f.write("\n")
                print(f"Graph written to {output_file}")
            else:
                print(dot)
        else:  # png, svg, pdf
            if not output_file:
                print(f"Error: --output FILE is required for --format {fmt}")
                sys.exit(1)
            if not shutil.which("dot"):
                print(
                    "Error: 'dot' command not found. "
                    "Install graphviz: sudo apt install graphviz"
                )
                sys.exit(1)
            result = subprocess.run(
                ["dot", f"-T{fmt}", "-o", output_file],
                input=dot,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                print(f"Error: graphviz dot command failed:\n{result.stderr}")
                sys.exit(1)
            print(f"Graph written to {output_file}")

    elif args.command == "map":
        from rich.console import Console

        from .engine.dependency_graph import TemplateDependencyGraph
        from .engine.pipeline_map import (
            build_pipeline_map,
            render_flat,
            render_grouped,
            render_tree,
        )

        graph = TemplateDependencyGraph()
        items = build_pipeline_map(include_ok=args.all)

        console = Console(no_color=args.no_color)

        if args.format == "flat":
            render_flat(items, console=console)
        elif args.format == "grouped":
            render_grouped(items, console=console, graph=graph)
        else:
            render_tree(
                items,
                console=console,
                graph=graph,
                reverse=args.reverse,
            )

        any_stale = any(it.status != "ok" for it in items)
        if any_stale:
            sys.exit(1)

    elif args.command == "doctor":
        from .engine.doctor import run_doctor

        categories = getattr(args, "category", None)
        template_filter = getattr(args, "template", None)
        last_days = getattr(args, "last", 30)
        do_fix = getattr(args, "fix", False)
        skip_confirm = getattr(args, "yes", False)
        validations_file = getattr(args, "validations_file", None)
        validations_config = Path(validations_file) if validations_file else None
        calendar_name = getattr(args, "calendar", "B3")

        try:
            report = run_doctor(
                categories=categories,
                template_filter=template_filter,
                last_days=last_days,
                validations_config=validations_config,
                calendar_name=calendar_name,
            )
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(2)

        _print_doctor_report(report)

        if do_fix and report.fixable():
            if not skip_confirm:
                answer = (
                    input(f"\nApply {len(report.fixable())} fix(es)? [y/N] ")
                    .strip()
                    .lower()
                )
                if answer != "y":
                    print("Aborted.")
                    sys.exit(0)
            for issue in report.fixable():
                if issue.fix_fn is not None:
                    try:
                        issue.fix_fn()
                        print(f"  Fixed: {issue.code}")
                    except Exception as exc:
                        print(f"  Error fixing {issue.code}: {exc}")
        elif do_fix:
            print("\nNo fixable issues found.")

        if report.errors():
            sys.exit(1)

    elif args.command == "cache":
        if args.cache_command == "drop":
            from .engine.exceptions import CacheError

            cm = CacheManager()
            meta_dict = cm._load_meta_dict_by_id(args.meta_id)
            if meta_dict is None:
                print(f"No cache entry with id {args.meta_id!r}", file=sys.stderr)
                sys.exit(1)

            print(
                f"About to drop cache entry {args.meta_id}:\n"
                f"  template: {meta_dict['template']}\n"
                f"  timestamp: {meta_dict['timestamp']}\n"
                f"  downloaded files: {len(meta_dict['downloaded_files'])}"
            )
            if not args.yes:
                reply = input("Proceed? [y/N]: ").strip().lower()
                if reply not in ("y", "yes"):
                    print("Aborted.")
                    sys.exit(0)

            try:
                cm.drop(args.meta_id)
            except CacheError as e:
                print(str(e), file=sys.stderr)
                sys.exit(1)
            print(f"Dropped cache entry {args.meta_id}.")
        else:
            parser_cache.print_help()
            sys.exit(1)


if __name__ == "__main__":
    main()
