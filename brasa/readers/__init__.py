
from .helpers import (
    read_json,
    read_b3_lending_trades,
    read_b3_otc_trade_information,
    read_b3_cotahist,
    read_b3_bvbg086,
    read_b3_bvbg028,
    read_b3_cdi,
    read_b3_futures_settlement_prices,
    read_b3_trades_intraday,
)

def null_reader(*args, **kwargs):
    return None