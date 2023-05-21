
from ..parsers.b3.cdi import read_b3_cdi
from ..parsers.b3.bvbg028 import read_b3_bvbg028
from ..parsers.b3.futures_settlement_prices import read_b3_futures_settlement_prices
from .helpers import read_json, read_csv

def null_reader(*args, **kwargs):
    return None