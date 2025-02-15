import argparse
from datetime import datetime


from . import download_marketdata, process_marketdata, process_etl, retrieve_template
from .util import DateRangeParser
from .engine import CacheManager
from .queries import BrasaDB

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest="command", title="Commands")

parser_setup = subparsers.add_parser("setup", help="setup brasa: create cache directories and metadata.db")

parser_download = subparsers.add_parser("download", help="download market data")
parser_download.add_argument(
    "-d", "--date", "--date-range", nargs="+", help="specify date or date range to download and process market data"
)
parser_download.add_argument(
    "--calendar",
    help="specify calendar to be used for creating date range",
    default="B3",
    choices=["B3", "ANBIMA", "actual"],
)
parser_download.add_argument("template", nargs="+", help="template names")

parser_process = subparsers.add_parser("process", help="process market data - transform raw data to parquet files")
parser_process.add_argument("template", nargs="+", help="template names")

parser_show = subparsers.add_parser("list", help="list available templates")
parser_show.add_argument("choice", choices=["templates"])

parser_create_views = subparsers.add_parser("create-views", help="create all views in brasa database")

parser_create_view = subparsers.add_parser("create-view", help="create specific view in brasa database")
parser_create_view.add_argument("template", nargs="+", help="template names")


if __name__ == "__main__":
    args = parser.parse_args()
    if args.command == "setup":
        man = CacheManager()
    elif args.command == "download":
        if len(args.date) == 1:
            date_range = DateRangeParser(args.calendar).parse(args.date[0])
        else:
            date_range = [datetime.strptime(d, "%Y-%m-%d") for d in args.date]
        for template in args.template:
            download_marketdata(template, refdate=date_range)
    elif args.command == "process":
        for template in args.template:
            _template = retrieve_template(template)
            if _template.is_etl:
                process_etl(template)
            else:
                process_marketdata(template)
    elif args.command == "create-views":
        BrasaDB.create_views()
    elif args.command == "create-view":
        for template in args.template:
            BrasaDB.create_view(template)
