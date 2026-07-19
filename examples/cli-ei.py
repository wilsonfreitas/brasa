from datetime import datetime

import brasa
from brasa.util import DateRange

period = DateRange(
    start=datetime(2026, 1, 19), end=datetime(2026, 1, 23), calendar="B3"
)

# brasa.download_marketdata("b3-bvbg087", refdate=datetime(2026, 1, 19), reprocess=True)
# brasa.process_marketdata("b3-bvbg087")
# brasa.download_marketdata("b3-bvbg028", refdate=period)
# brasa.download_marketdata("b3-bvbg086", refdate=period)
# brasa.download_marketdata("b3-otc-trade-information", refdate=period)
# brasa.download_marketdata("b3-economic-indicators-fwf", refdate=period)

# brasa.process_marketdata("b3-futures-settlement-prices", reprocess=True)
# brasa.download_marketdata("b3-economic-indicators-fwf", refdate=period)
# brasa.download_marketdata("b3-bvbg086", refdate=period)
# brasa.download_marketdata("b3-bvbg087", refdate=period)

# brasa.process_etl("b3-futures-settlement-prices-consolidated")
# brasa.process_etl("b3-futures-dap")

# brasa.download_marketdata(
#     "b3-company-info", issuingCompany=brasa.get_symbols("company")
# )

# brasa.download_marketdata("b3-indexes-composition")
# brasa.process_marketdata("b3-indexes-composition", reprocess=True)
# brasa.process_etl("b3-indexes-composition-consolidated")

# brasa.download_marketdata(
#     "b3-indexes-theoretical-portfolio",
#     index=brasa.get_symbols("index"),
# )

# brasa.download_marketdata(
#     "b3-indexes-theoretical-portfolio",
#     index=["IBOV"],
#     reprocess=True,
# )

# brasa.download_marketdata(
#     "b3-indexes-historical-prices",
#     index=brasa.get_symbols("index"),
#     year=[2026],
#     reprocess=True,
# )

# brasa.download_marketdata(
#     "b3-indexes-historical-prices",
#     index=["IBOV"],
#     year=[2026],
#     reprocess=True,
# )

# brasa.process_marketdata("b3-cash-dividends", reprocess=True, max_workers=1)
# brasa.process_marketdata("b3-bvbg086", reprocess=True)

# brasa.process_etl("b3-equities-register")
# brasa.process_etl("b3-futures-register")

# brasa.download_marketdata(
#     "b3-indexes-current-portfolio", index=brasa.get_symbols("index")
# )

# brasa.download_marketdata(
#     "b3-listed-funds", typeFund=["ETF", "ETF-CRIPTO", "ETF-RF", "ETF-FII"]
# )

# brasa.process_marketdata("b3-listed-funds", reprocess=True)

# cvm-companies-registration
# brasa.download_marketdata("cvm-companies-registration")
# brasa.process_marketdata("cvm-companies-registration", reprocess=True)
# brasa.process_marketdata("b3-cotahist-daily", reprocess=True)
# brasa.process_marketdata("b3-cotahist-yearly", reprocess=True)

# brasa.process_marketdata("b3-company-details", reprocess=True, max_workers=10)
# brasa.process_etl("brasa-companies")
# brasa.download_marketdata("b3-companies-capital")
brasa.process_marketdata("b3-companies-capital", reprocess=True)
