from datetime import datetime
import io

from regexparser import PortugueseRulesParser, GenericParser


class Parser:
    mode = "r"
    encoding: str = "utf-8"

    def _open(self, fp, func):
        if isinstance(fp, io.IOBase):
            x = func(fp)
        else:
            with open(fp, self.mode, encoding=self.encoding) as f:
                x = func(f)
        return x


class PortugueseRulesParser2(PortugueseRulesParser):
    def parseInteger(self, text, match):
        r"^\d+$"
        return int(match.group())

    def parseND(self, text, match):
        r"^N\/D$"
        return None

    def parseEmpty(self, text, match):
        r"^$"
        return None

    def parseEmpty2(self, text, match):
        r"^--$"
        return None

    def parseDate_ptBR(self, text, match):
        r"(\d{2})/(\d{2})/(\d{4})"
        return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))

    def parseDate_YYYYMMDD(self, text, match):
        r"(\d{4})(\d{2})(\d{2})"
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))


def convert_csv_to_dict(file, sep=";", encoding="utf-8"):
    parser = GenericParser()
    for ix, line in enumerate(file):
        line = line.decode(encoding).strip()
        if ix == 0:
            hdr = [field.lower() for field in line.split(sep)]
        vals = [parser.parse(val) for val in line.split(sep)]
        yield dict(zip(hdr, vals))


def read_fwf(con, widths, colnames=None, skip=0, parse_fun=lambda x: x):
    """read and parse fixed width field files"""
    colpositions = []
    x = 0
    line_len = sum(widths)
    for w in widths:
        colpositions.append((x, x + w))
        x = x + w

    colnames = ["V{}".format(ix + 1) for ix in range(len(widths))] if colnames is None else colnames

    terms = []
    for ix, line in enumerate(con):
        if ix < skip:
            continue
        line = line.strip()
        if len(line) != line_len:
            continue
        fields = [line[dx[0] : dx[1]].strip() for dx in colpositions]
        obj = dict((k, v) for k, v in zip(colnames, fields))
        terms.append(parse_fun(obj))

    return terms


def float_or_none(val):
    """Converts val to float or returns None if it's not possible."""
    try:
        return float(val)
    except:
        return None


def str_or_none(val):
    return str(val) if val else None
