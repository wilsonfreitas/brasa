"""Unit tests for generate_checksum_from_zip.

Uses in-memory zips built with zipfile.ZipFile over BytesIO so each test
controls both content and non-deterministic metadata (date_time, OS byte
via ZipInfo, entry order). No filesystem I/O.
"""

import zipfile
from io import BytesIO

from brasa.util import generate_checksum_from_zip


def _make_zip(
    entries: list[tuple[str, bytes]],
    date_time: tuple[int, int, int, int, int, int] = (2020, 1, 1, 0, 0, 0),
) -> BytesIO:
    """Build an in-memory zip from an ordered list of (name, content) pairs.

    Using a list (not a dict) preserves insertion order so tests can
    deliberately reorder entries in the central directory.
    """
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries:
            info = zipfile.ZipInfo(name, date_time=date_time)
            zf.writestr(info, data)
    buf.seek(0)
    return buf


def test_identical_content_different_metadata_same_checksum():
    z1 = _make_zip(
        [("a.txt", b"hello"), ("b.txt", b"world")],
        date_time=(2020, 1, 1, 0, 0, 0),
    )
    z2 = _make_zip(
        [("a.txt", b"hello"), ("b.txt", b"world")],
        date_time=(2024, 6, 15, 12, 30, 45),
    )
    assert z1.getvalue() != z2.getvalue()  # raw bytes differ
    assert generate_checksum_from_zip(z1) == generate_checksum_from_zip(z2)
