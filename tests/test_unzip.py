
import os
import tempfile
from brasa.parsers.util import unzip_and_get_content, unzip_file_to


def test_unzip():
    dest = unzip_file_to('data/TS190910.ex_', 'data')
    assert os.path.exists(dest)

    destdir = tempfile.gettempdir()
    dest = unzip_file_to('data/TS190910.ex_', destdir)
    assert os.path.exists(dest)


def test_unzip_and_get_content():
    rawdata = unzip_and_get_content('data/IR210423.zip')
    assert rawdata is not None

    rawdata = unzip_and_get_content('data/TS190910.ex_')
    assert rawdata is not None
