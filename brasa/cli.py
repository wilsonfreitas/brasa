
import argparse
import brasa

from brasa.util import DateRangeParser

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest="command", title="Commands")
parser_download = subparsers.add_parser("download", help="download market data")
parser_download.add_argument("-d", "--date", "--date-range", help="specify date or date range to download and process market data")
parser_download.add_argument("--calendar", help="specify calendar to be used for creating date range", default="B3",
                             choices=["B3", "ANBIMA", "actual"])
parser_download.add_argument("template", nargs="+", help="template names")

parser_process = subparsers.add_parser("process", help="process market data - transform raw data to parquet files")
parser_process.add_argument("template", nargs="+", help="template names")

parser_show = subparsers.add_parser("list", help="list available templates")
parser_show.add_argument("choice", choices=["templates"])


if __name__ == "__main__":
    args = parser.parse_args()
    if args.command == "download":
        date_range = DateRangeParser(args.calendar).parse(args.date)
        for template in args.template:
            brasa.download_marketdata(template, refdate=date_range)
    elif args.command == "process":
        for template in args.template:
            brasa.process_marketdata(template)
