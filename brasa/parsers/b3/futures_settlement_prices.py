from datetime import datetime

from bizdays import Calendar


def maturity2date_newcode(x: str, cal: Calendar, expr: str) -> datetime:
    """Converts a maturity code to a date.

    The new code is a single letter, as in "F" for January.
    This code started to be used in 2007.
    """
    year = int(x[-2:]) + 2000
    month = code2month_newcode(x[0])
    return cal.getdate(expr, year, month)


def maturity2date_oldcode(x: str, cal: Calendar, expr: str) -> datetime:
    """Converts a maturity code to a date.

    The old code is a three-letter code, as in "JAN" for January.
    This code was used until 2007.
    """
    year = int(x[-1:]) + 2000
    month = code2month_oldcode(x[:3])
    return cal.getdate(expr, year, month)


def maturity2date(x: str, cal: Calendar, expr: str = "first day") -> datetime:
    maturity_code = x[-3:]
    if len(maturity_code) == 3:
        return maturity2date_newcode(maturity_code, cal, expr)
    else:
        return maturity2date_oldcode(maturity_code, cal, expr)


def code2month(code: str) -> int:
    """Converts a month code to a month number.

    The code can be a single letter, as in "F" for January,
    or a three-letter code, as in "JAN" for January.
    """
    if len(code) == 1:
        return code2month_newcode(code)
    else:
        return code2month_oldcode(code)


def code2month_newcode(code: str) -> int:
    """Converts a month code to a month number.

    The new code is a single letter, as in "F" for January.
    This code started to be used in 2007.
    """
    month_codes = "FGHJKMNQUVXZ"
    return month_codes.index(code) + 1


def code2month_oldcode(code: str) -> int:
    """Converts a month code to a month number.

    The old code is a three-letter code, as in "JAN" for January.
    This code was used until 2007.
    """
    month_codes = [
        "JAN",
        "FEV",
        "MAR",
        "ABR",
        "MAI",
        "JUN",
        "JUL",
        "AGO",
        "SET",
        "OUT",
        "NOV",
        "DEZ",
    ]
    return month_codes.index(code) + 1
