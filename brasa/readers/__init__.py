
from .helpers import (
    read_json,
    read_b3_lending_trades,
    read_b3_otc_trade_information,
    read_b3_cotahist,
    read_b3_bvbg087,
    read_b3_bvbg086,
    read_b3_bvbg028,
    read_b3_cdi,
    read_b3_futures_settlement_prices,
    read_b3_trades_intraday,
    read_b3_economic_indicators_price,
    read_b3_equity_volatility_surface,
    read_b3_equity_options,
    read_b3_company_info,
    read_b3_company_details,
    read_b3_cash_dividends,
    read_b3_index_theoretical_portfolio,
)

def null_reader(*args, **kwargs):
    return None