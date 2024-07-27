# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

from datetime import datetime
import brasa
from brasa.util import DateRange

period = DateRange(start=datetime(2024, 1, 1), end=datetime.today(), calendar="B3")

brasa.download_marketdata("b3-bvbg087", refdate=period)
brasa.process_marketdata("b3-bvbg087")
brasa.process_etl("b3-indexes-returns")

brasa.download_marketdata("b3-bvbg028", refdate=period)
brasa.process_marketdata("b3-bvbg028")
brasa.process_etl("b3-futures-register")
brasa.process_etl("b3-equities-register")
brasa.process_etl("b3-equities-spot-market")

brasa.download_marketdata("b3-bvbg086", refdate=period)
brasa.process_marketdata("b3-bvbg086")

brasa.download_marketdata("b3-futures-settlement-prices", refdate=period)
brasa.process_marketdata("b3-futures-settlement-prices")
brasa.process_etl("b3-futures-di1")
brasa.process_etl("b3-futures-ddi")
brasa.process_etl("b3-futures-ddi-first-generic")
brasa.process_etl("b3-futures-dol")
brasa.process_etl("b3-futures-dol-first-generic")
brasa.process_etl("b3-futures-wdo")
brasa.process_etl("b3-futures-wdo-first-generic")
brasa.process_etl("b3-futures-wdo-adjusted")
brasa.process_etl("b3-futures-wdo-adjusted-returns")
brasa.process_etl("b3-futures-dap")
brasa.process_etl("b3-futures-dap-first-generic")
brasa.process_etl("b3-futures-frc")
brasa.process_etl("b3-futures-win")
brasa.process_etl("b3-futures-win-first-generic")
brasa.process_etl("b3-futures-win-adjusted")
brasa.process_etl("b3-futures-win-adjusted-returns")
brasa.process_etl("bcb-data")
brasa.process_etl("b3-curves-di1")
brasa.process_etl("b3-curves-di1-standard")
brasa.process_etl("b3-curves-di1-standard-returns")
brasa.process_etl("b3-curves-dap")
brasa.process_etl("b3-curves-dap-standard")
brasa.process_etl("b3-curves-dap-standard-returns")

brasa.download_marketdata("b3-cotahist-daily", refdate=period)
brasa.process_marketdata("b3-cotahist-daily")
brasa.process_etl("b3-cotahist")
brasa.process_etl("b3-equities-returns")

brasa.download_marketdata("b3-lending-trades", refdate=period)
brasa.process_marketdata("b3-lending-trades")

brasa.download_marketdata("b3-otc-trade-information", refdate=period)
brasa.process_marketdata("b3-otc-trade-information")

brasa.download_marketdata("b3-economic-indicators-price", refdate=period)
brasa.process_marketdata("b3-economic-indicators-price")

brasa.download_marketdata("b3-equities-volatility-surface", refdate=period)
brasa.process_marketdata("b3-equities-volatility-surface")

# brasa.download_marketdata("b3-companies-options", refdate=period)

brasa.download_marketdata("b3-indexes-composition")
brasa.process_marketdata("b3-indexes-composition")

brasa.download_marketdata("b3-indexes-theoretical-portfolio", index=brasa.get_symbols("index"))
brasa.process_marketdata("b3-indexes-theoretical-portfolio")

brasa.download_marketdata("b3-company-info", issuingCompany=brasa.get_symbols("company"))
brasa.process_marketdata("b3-company-info")
brasa.process_etl("b3-companies-info")
# depends on b3-companies-info
brasa.download_marketdata("b3-company-details", codeCVM=brasa.get_symbols("company-cvm-code"))
brasa.process_marketdata("b3-company-details")
brasa.process_etl("b3-companies-details")
# depends on b3-companies-details
brasa.download_marketdata("b3-cash-dividends", tradingName=brasa.get_symbols("company-trading-name"))
brasa.process_marketdata("b3-cash-dividends")
brasa.process_etl("b3-companies-properties")
brasa.process_etl("b3-equity-symbols-properties")
brasa.process_etl("b3-companies-cash-dividends")
brasa.process_etl("b3-companies-stock-dividends")
brasa.process_etl("b3-companies-subscriptions")

brasa.download_marketdata("b3-listed-fixed-income-etfs")
brasa.process_marketdata("b3-listed-fixed-income-etfs")
brasa.download_marketdata("b3-listed-stock-etfs")
brasa.process_marketdata("b3-listed-stock-etfs")
brasa.download_marketdata("b3-listed-reits")
brasa.process_marketdata("b3-listed-reits")
brasa.process_etl("b3-listed-funds")

brasa.process_etl("brasa-returns")

brasa.download_marketdata("b3-trades-intraday", refdate=period)
brasa.process_marketdata("b3-trades-intraday")

brasa.process_etl("bcb-currency-data")
brasa.process_etl("bcb-currencies-returns")
brasa.process_etl("b3-equities-adjusted-prices")
brasa.process_etl("b3-indexes-adjusted-prices")
brasa.process_etl("brasa-ohlc-prices")
brasa.process_etl("brasa-prices")
