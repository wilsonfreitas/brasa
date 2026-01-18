import argparse
import json
import sys
from datetime import datetime

from . import download_marketdata, process_etl, process_marketdata, retrieve_template
from .engine import CacheManager, Verbosity, sync_catalog_from_disk
from .queries import BrasaDB, describe_dataset, get_dataset, list_datasets
from .util import DateRangeParser


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


parser = argparse.ArgumentParser(
    description="Brasa CLI for downloading and processing market data"
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

parser_create_view = subparsers.add_parser(
    "create-view", help="create specific view in brasa database"
)
parser_create_view.add_argument("template", nargs="+", help="template names")

parser_query = subparsers.add_parser(
    "query", help="execute read-only SQL queries on brasa database"
)
parser_query.add_argument("sql_query", nargs=1, help="SQL query to be executed")
parser_query.add_argument(
    "-o", "--output", nargs=1, help="SQL query to be executed", default="display"
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
        BrasaDB.create_views()
    elif args.command == "create-view":
        for template in args.template:
            BrasaDB.create_view(template)
    elif args.command == "query":
        q = BrasaDB.get_connection().sql(args.sql_query[0])
        output = args.output[0]
        if output == "display":
            print(q)
        elif output.endswith(".csv"):
            q.df().to_csv(output, sep=",", encoding="utf-8", index=False)
        elif output.endswith(".json"):
            q.df().to_json(output, index=False)
        elif output.endswith(".parquet"):
            q.df().to_parquet(output, index=False)
        elif output.endswith(".orc"):
            q.df().to_orc(output, index=False)
        elif output.endswith(".xls") or output.endswith(".xlsx"):
            q.df().to_excel(output, index=False)
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
            print(df.to_string())
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
