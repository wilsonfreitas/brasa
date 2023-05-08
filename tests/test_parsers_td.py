
from kyd.parsers.td import TesouroDiretoHistoricalDataParser


def test_TesouroDiretoHistoricalDataParser():
    fname = 'data/LFT_2022.xls'
    x = TesouroDiretoHistoricalDataParser(fname)
    assert len(x.data) > 0

    with open(fname, TesouroDiretoHistoricalDataParser.mode, encoding=TesouroDiretoHistoricalDataParser.encoding) as fp:
        x = TesouroDiretoHistoricalDataParser(fp)
    assert len(x.data) > 0
