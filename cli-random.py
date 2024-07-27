# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

from datetime import datetime
import brasa
from brasa.util import DateRange

# period = DateRange(start=datetime(2024, 2, 1), end=datetime.today(), calendar="B3")
# period = datetime(2024, 1, 31)
# brasa.download_marketdata("b3-bvbg028", refdate=period)
# brasa.process_marketdata("b3-bvbg028")
# brasa.process_etl("b3-equities-returns")
# brasa.process_etl("brasa-returns")

# brasa.download_marketdata("b3-company-info", issuingCompany=brasa.get_symbols("company"))
# with open("asset_names.txt") as f:
#     asset_names = [s.strip() for s in f.readlines()]
# # print(asset_names)
# brasa.download_marketdata("b3-company-info", issuingCompany=asset_names)
# brasa.process_marketdata("b3-company-info")
# brasa.process_etl("b3-companies-info")

# depends on b3-companies-info
# brasa.download_marketdata("b3-company-details", codeCVM=brasa.get_symbols("company-cvm-code"))
# brasa.process_marketdata("b3-company-details")
# brasa.process_etl("b3-companies-details")
# depends on b3-companies-details
# brasa.download_marketdata("b3-cash-dividends", tradingName=brasa.get_symbols("company-trading-name"))
# brasa.process_marketdata("b3-cash-dividends")
# brasa.process_etl("b3-companies-properties")
# brasa.process_etl("b3-equity-symbols-properties")
# brasa.process_etl("b3-companies-cash-dividends")
# brasa.process_etl("b3-companies-stock-dividends")
# brasa.process_etl("b3-companies-subscriptions")

# brasa.process_etl("b3-equities-adjusted-prices")
# brasa.process_etl("b3-indexes-adjusted-prices")
# brasa.process_etl("brasa-prices")


# brasa.process_etl("bcb-data")
# brasa.process_etl("bcb-currency-data")
# brasa.process_etl("bcb-currencies-returns")

# brasa.process_etl("b3-futures-wdo")
# brasa.process_etl("b3-futures-wdo-adjusted")
# brasa.process_etl("b3-futures-win-adjusted")
# brasa.process_etl("b3-futures-win-adjusted-returns")
# brasa.process_etl("b3-futures-wdo-adjusted-returns")
# brasa.process_etl("brasa-returns")

# period = datetime(2024, 7, 5)
# brasa.download_marketdata("b3-bvbg028", refdate=period)
# brasa.process_marketdata("b3-bvbg028")

# period = datetime(2024, 1, 2)
period = DateRange(start=datetime(2024, 1, 1), end=datetime.today(), calendar="B3")
brasa.download_marketdata("b3-cotahist-daily", refdate=period)
brasa.process_marketdata("b3-cotahist-daily")
# brasa.download_marketdata("b3-loan-balance", refdate=period)
