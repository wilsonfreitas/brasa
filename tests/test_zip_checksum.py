"""Unit tests for generate_checksum_from_zip.

Uses in-memory zips built with zipfile.ZipFile over BytesIO so each test
controls both content and non-deterministic metadata (date_time, OS byte
via ZipInfo, entry order). No filesystem I/O.
"""

import zipfile
from io import BytesIO

import pytest

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


def test_reordered_entries_same_checksum():
    z1 = _make_zip([("a.txt", b"one"), ("b.txt", b"two"), ("c.txt", b"three")])
    z2 = _make_zip([("c.txt", b"three"), ("a.txt", b"one"), ("b.txt", b"two")])
    assert generate_checksum_from_zip(z1) == generate_checksum_from_zip(z2)


def test_different_content_different_checksum():
    z1 = _make_zip([("a.txt", b"hello")])
    z2 = _make_zip([("a.txt", b"HELLO")])
    assert generate_checksum_from_zip(z1) != generate_checksum_from_zip(z2)


def test_adding_a_file_changes_checksum():
    z1 = _make_zip([("a.txt", b"x")])
    z2 = _make_zip([("a.txt", b"x"), ("b.txt", b"y")])
    assert generate_checksum_from_zip(z1) != generate_checksum_from_zip(z2)


def test_nested_zip_stable_across_container_rebuilds():
    inner1 = _make_zip(
        [("inner.txt", b"data")], date_time=(2020, 1, 1, 0, 0, 0)
    ).getvalue()
    inner2 = _make_zip(
        [("inner.txt", b"data")], date_time=(2024, 6, 15, 12, 30, 45)
    ).getvalue()
    assert inner1 != inner2
    outer1 = _make_zip([("nested.zip", inner1)], date_time=(2020, 1, 1, 0, 0, 0))
    outer2 = _make_zip([("nested.zip", inner2)], date_time=(2024, 6, 15, 12, 30, 45))
    assert generate_checksum_from_zip(outer1) == generate_checksum_from_zip(outer2)


def test_nested_zip_different_inner_content_different_checksum():
    inner_a = _make_zip([("x.txt", b"A")]).getvalue()
    inner_b = _make_zip([("x.txt", b"B")]).getvalue()
    outer_a = _make_zip([("nested.zip", inner_a)])
    outer_b = _make_zip([("nested.zip", inner_b)])
    assert generate_checksum_from_zip(outer_a) != generate_checksum_from_zip(outer_b)


def test_fp_seek_restored_to_zero():
    z = _make_zip([("a.txt", b"hello")])
    generate_checksum_from_zip(z)
    assert z.tell() == 0


def test_non_zip_fp_raises():
    fp = BytesIO(b"not a zip file at all")
    with pytest.raises(zipfile.BadZipFile):
        generate_checksum_from_zip(fp)


def test_recursion_depth_cap():
    payload = _make_zip([("leaf.txt", b"bottom")]).getvalue()
    for _ in range(10):
        payload = _make_zip([("nested.zip", payload)]).getvalue()
    with pytest.raises(RecursionError):
        generate_checksum_from_zip(BytesIO(payload))
