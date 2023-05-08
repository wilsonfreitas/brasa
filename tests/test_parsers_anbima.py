
from kyd.parsers.anbima import TPFParser, VnaTPFParser, DebenturesParser


def test_AnbimaTPF():
    x = TPFParser('data/ANBIMA_TPF_2019-01-02.txt')
    assert isinstance(x.data, list)
    assert len(x.data) > 0

    with open('data/ANBIMA_TPF_2019-01-02.txt', "r", encoding=TPFParser.encoding) as fp:
        x = TPFParser(fp)
    assert isinstance(x.data, list)
    assert len(x.data) > 0


def test_AnbimaVnaTPF():
    x = VnaTPFParser('data/ANBIMA_TPF_VNA_2019-01-06.html')
    assert isinstance(x.data, list)
    assert len(x.data) > 0

    with open('data/ANBIMA_TPF_VNA_2019-01-06.html', "r", encoding=VnaTPFParser.encoding) as fp:
        x = VnaTPFParser(fp)
    assert isinstance(x.data, list)
    assert len(x.data) > 0


def test_AnbimaDebentures():
    x = DebenturesParser('data/deb_2021-04-20.txt')
    assert isinstance(x.data, list)
    assert len(x.data) > 0
    with open('data/deb_2021-04-20.txt', "r", encoding=DebenturesParser.encoding) as fp:
        x = DebenturesParser(fp)
    assert isinstance(x.data, list)
    assert len(x.data) > 0
