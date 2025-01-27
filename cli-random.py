# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

from datetime import datetime
import brasa
from brasa.util import DateRange

# brasa.download_marketdata("b3-cash-dividends", tradingName="BRADESCO")
brasa.download_marketdata("b3-cash-dividends", tradingName=brasa.get_symbols("company-trading-name"))
# brasa.process_marketdata("b3-cash-dividends")
# brasa.process_etl("b3-companies-properties")
# brasa.process_etl("b3-equity-symbols-properties")
# brasa.process_etl("b3-companies-cash-dividends")
# brasa.process_etl("b3-companies-stock-dividends")
# brasa.process_etl("b3-companies-subscriptions")
