import argparse
import sys
from datetime import datetime

from . import download_marketdata, process_etl, process_marketdata, retrieve_template
from .engine import CacheManager, Verbosity
from .queries import BrasaDB, get_dataset
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
