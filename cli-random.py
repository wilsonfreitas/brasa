# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

from datetime import datetime

import brasa
from brasa.util import DateRange

period = DateRange(start=datetime(2026, 1, 1), end=datetime.today(), calendar="B3")

brasa.download_marketdata("b3-bvbg087", refdate=period)

brasa.download_marketdata("b3-bvbg028", refdate=period)

brasa.download_marketdata("b3-bvbg086", refdate=period)

# brasa.download_marketdata("b3-futures-settlement-prices", refdate=period)

brasa.download_marketdata("b3-cotahist-daily", refdate=period)

# brasa.download_marketdata("b3-lending-trades", refdate=period)

brasa.download_marketdata("b3-otc-trade-information", refdate=period)

# brasa.download_marketdata("b3-economic-indicators-price", refdate=period)

brasa.download_marketdata("b3-economic-indicators-fwf", refdate=period)

# brasa.download_marketdata("b3-equities-volatility-surface", refdate=period)

# brasa.download_marketdata("b3-companies-options", refdate=period)

# brasa.download_marketdata("b3-company-info", issuingCompany=brasa.get_symbols("company"))
# brasa.process_marketdata("b3-company-info")
# brasa.process_etl("b3-companies-info")
# # depends on b3-companies-info
# brasa.download_marketdata("b3-company-details", codeCVM=brasa.get_symbols("company-cvm-code"))
# brasa.process_marketdata("b3-company-details")
# brasa.process_etl("b3-companies-details")
# # depends on b3-companies-details
# brasa.download_marketdata("b3-cash-dividends", tradingName=brasa.get_symbols("company-trading-name"))
# brasa.process_marketdata("b3-cash-dividends")
# brasa.process_etl("b3-companies-properties")
# brasa.process_etl("b3-equity-symbols-properties")
# brasa.process_etl("b3-companies-cash-dividends")
# brasa.process_etl("b3-companies-stock-dividends")
# brasa.process_etl("b3-companies-subscriptions")

brasa.download_marketdata(
    "b3-listed-funds", typeFund=["ETF", "ETF-CRIPTO", "ETF-RF", "ETF-FII"]
)

brasa.download_marketdata("b3-trades-intraday", refdate=period)

brasa.download_marketdata("b3-indexes-composition")

brasa.download_marketdata(
    "b3-indexes-theoretical-portfolio", index=brasa.get_symbols("index")
)

brasa.download_marketdata(
    "b3-indexes-current-portfolio", index=brasa.get_symbols("index")
)

brasa.download_marketdata(
    "b3-indexes-historical-prices",
    index=brasa.get_symbols("index"),
    year=[2026],
    reprocess=True,
)

# brasa.download_marketdata(
#     "b3-company-info", issuingCompany=brasa.get_symbols("company")
# )


# brasa.process_marketdata("b3-bvbg087")
# brasa.process_marketdata("b3-bvbg028")
# brasa.process_marketdata("b3-bvbg086")
# brasa.process_marketdata("b3-futures-settlement-prices")
# brasa.process_marketdata("b3-cotahist-daily")
# brasa.process_marketdata("b3-lending-trades")
# brasa.process_marketdata("b3-otc-trade-information")
# brasa.process_marketdata("b3-economic-indicators-price")
# brasa.process_marketdata("b3-economic-indicators-fwf")
# brasa.process_marketdata("b3-equities-volatility-surface")
# brasa.process_marketdata("b3-indexes-composition")
# brasa.process_marketdata("b3-indexes-theoretical-portfolio")
# brasa.process_marketdata("b3-listed-funds")
# brasa.process_marketdata("b3-trades-intraday")
