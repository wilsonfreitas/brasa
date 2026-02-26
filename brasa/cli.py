import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import download_marketdata, process_etl, process_marketdata, retrieve_template
from .engine import CacheManager, Verbosity, sync_catalog_from_disk
from .queries import BrasaDB, describe_dataset, get_dataset, list_datasets
from .util import DateRangeParser

# Command groups for organized help display
_COMMAND_GROUPS = {
    "Setup": ["setup"],
    "Execution": ["download", "process", "run"],
    "Templates": ["deps", "plan", "graph"],
    "Datasets": [
        "list-unprocessed",
        "list-datasets",
        "describe-dataset",
        "sync-catalog",
        "head",
    ],
    "Database": ["create-views", "create-view", "list-tables", "query"],
}


class _GroupedHelpFormatter(argparse.HelpFormatter):
    """HelpFormatter that displays subcommands organized into labeled sections."""

    def _format_action(self, action):
        if not isinstance(action, argparse._SubParsersAction):
            return super()._format_action(action)

        help_map = {a.dest: a.help or "" for a in action._choices_actions}

        grouped_cmds = {cmd for cmds in _COMMAND_GROUPS.values() for cmd in cmds}
        ungrouped = [c for c in action.choices if c not in grouped_cmds]

        parts = []
        for group_name, cmds in _COMMAND_GROUPS.items():
            available = [c for c in cmds if c in action.choices]
            if not available:
                continue
            parts.append(f"\n  {group_name}:\n")
            for cmd in available:
                parts.append(f"    {cmd:<24}{help_map.get(cmd, '')}\n")

        if ungrouped:
            parts.append("\n  Other:\n")
            for cmd in ungrouped:
                parts.append(f"    {cmd:<24}{help_map.get(cmd, '')}\n")

        return "".join(parts)


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


def _get_terminal_width(override_width: int | None = None) -> int:
    """Get terminal width for display.

    Args:
        override_width: Manual width override. If provided, uses this value.

    Returns:
        Terminal width in characters.
    """
    if override_width is not None:
        return override_width
    try:
        return shutil.get_terminal_size().columns
    except (AttributeError, ValueError):
        return 80  # Fallback default


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
    terminal_width = _get_terminal_width(width)

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


parser = argparse.ArgumentParser(
    description="Brasa CLI for downloading and processing market data",
    formatter_class=_GroupedHelpFormatter,
)

subparsers = parser.add_subparsers(dest="command", title="Commands")

parser_setup = subparsers.add_parser(
    "setup", help="setup brasa: create cache directories and metadata.db"
)

parser_download = subparsers.add_parser("download", help="download market data")
parser_download.add_argument(
    "-d",
    "--date",
    "--date-range",
    nargs="+",
    help="specify date or date range to download and process market data",
)
parser_download.add_argument(
    "--calendar",
    help="specify calendar to be used for creating date range",
    default="B3",
    choices=["B3", "ANBIMA", "actual"],
)
parser_download.add_argument("template", nargs="+", help="template names")
add_verbosity_args(parser_download)

parser_process = subparsers.add_parser(
    "process", help="process market data - transform raw data to parquet files"
)
parser_process.add_argument("template", nargs="+", help="template names")
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
parser_query.add_argument("sql_query", nargs=1, help="SQL query to be executed")
parser_query.add_argument(
    "-o",
    "--output",
    nargs=1,
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


if __name__ == "__main__":
    args = parser.parse_args()
    if args.command == "setup":
        man = CacheManager()
    elif args.command == "download":
        if len(args.date) == 1:
            date_range = DateRangeParser(args.calendar).parse(args.date[0])
        else:
            date_range = [datetime.strptime(d, "%Y-%m-%d") for d in args.date]
        verbosity = get_verbosity(args)
        report_file = getattr(args, "report", None)
        if verbosity != Verbosity.QUIET:
            print(
                "Status legend: .(passed) F(failed) E(error) "
                "S(skipped) D(duplicated) I(invalid) C(corrupted)"
            )
        for template in args.template:
            download_marketdata(
                template,
                refdate=date_range,
                verbosity=verbosity,
                report_file=report_file,
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
                    verbosity=verbosity,
                    report_file=report_file,
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
        q = BrasaDB.query(args.sql_query[0])
        output = args.output[0] if isinstance(args.output, list) else args.output

        if getattr(args, "verbose", False):
            # Show query execution info (could add EXPLAIN here)
            try:
                explain = BrasaDB.query(f"EXPLAIN {args.sql_query[0]}")
                print("Query Plan:")
                print(explain)
                print("\n" + "=" * 60 + "\n")
            except Exception:
                pass  # If EXPLAIN fails, just skip it

        if output == "display":
            print(q)
        elif output.endswith(".csv"):
            q.df().to_csv(output, sep=",", encoding="utf-8", index=False)
            print(f"Results saved to {output}")
        elif output.endswith(".json"):
            q.df().to_json(output, index=False)
            print(f"Results saved to {output}")
        elif output.endswith(".parquet"):
            q.df().to_parquet(output, index=False)
            print(f"Results saved to {output}")
        elif output.endswith(".orc"):
            q.df().to_orc(output, index=False)
            print(f"Results saved to {output}")
        elif output.endswith(".xls") or output.endswith(".xlsx"):
            q.df().to_excel(output, index=False)
            print(f"Results saved to {output}")
    elif args.command == "head":
        # Validate dataset format
        if "." not in args.dataset:
            print(
                "Error: Invalid dataset format. "
                "Use layer.dataset (e.g., input.b3-cotahist)"
            )
            sys.exit(1)

        parts = args.dataset.split(".", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            print(
                "Error: Invalid dataset format. "
                "Use layer.dataset (e.g., input.b3-cotahist)"
            )
            sys.exit(1)

        layer, dataset_name = parts

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
        elif output.endswith(".csv"):
            df.to_csv(output, sep=",", encoding="utf-8", index=False)
        elif output.endswith(".json"):
            df.to_json(output, orient="records", indent=2)
        elif output.endswith(".parquet"):
            df.to_parquet(output, index=False)
        elif output.endswith(".xls") or output.endswith(".xlsx"):
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
        # Validate dataset format
        if "." not in args.dataset:
            print(
                "Error: Invalid dataset format. "
                "Use layer.dataset (e.g., input.b3-cotahist)"
            )
            sys.exit(1)

        parts = args.dataset.split(".", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            print(
                "Error: Invalid dataset format. "
                "Use layer.dataset (e.g., input.b3-cotahist)"
            )
            sys.exit(1)

        layer, dataset_name = parts

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
