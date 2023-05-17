
from datetime import datetime

import pandas as pd
from brasa.engine import download_and_read_marketdata


def futures_settlement_prices_get(refdate: datetime) -> pd.DataFrame:
    return download_and_read_marketdata("b3-futures-settlement-prices", refdate=refdate)


def bvbg028_get(refdate: datetime) -> dict[str, pd.DataFrame]:
    return download_and_read_marketdata("b3-bvbg028", refdate=refdate)

