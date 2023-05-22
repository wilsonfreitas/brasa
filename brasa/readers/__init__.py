
from .helpers import (
    read_json,
    read_csv,
    read_b3_cotahist,
    read_b3_bvbg086,
    read_b3_cdi,
    read_b3_futures_settlement_prices,
)

def null_reader(*args, **kwargs):
    return None