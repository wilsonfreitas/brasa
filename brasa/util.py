import hashlib
import logging
import os
import pickle
import warnings
import zipfile
from tempfile import gettempdir
from typing import IO


class SuppressUserWarnings:
    def __enter__(self):
        warnings.filterwarnings("ignore", category=UserWarning)
    
    def __exit__(self, exc_type, exc_value, traceback):
        warnings.filterwarnings("default", category=UserWarning)

    
def generate_checksum_for_template(template: str, args: dict, extra_key: str="") -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    """
    t = tuple(sorted(args.items(), key=lambda x: x[0]))
    obj = (template, t)
    if extra_key:
        obj = (template, t, extra_key)
    return hashlib.md5(pickle.dumps(obj)).hexdigest()


def generate_checksum_from_file(fp: IO) -> str:
    file_hash = hashlib.md5()
    while chunk := fp.read(8192):
        file_hash.update(chunk)
    fp.seek(0)
    return file_hash.hexdigest()


def unzip_file_to(fname, dest) -> list:
    zf = zipfile.ZipFile(fname)
    names = zf.namelist()
    for name in names:
        logging.debug("zipped file %s", name)
        zf.extract(name, dest)
    zf.close()
    return [os.path.join(dest, name) for name in names]


def unzip_recursive(fname):
    if isinstance(fname, str) and fname.lower().endswith(".zip"):
        fname = unzip_file_to(fname, gettempdir())
        return unzip_recursive(fname)
    elif isinstance(fname, list) and len(fname) == 1 and fname[0].lower().endswith(".zip"):
        fname = unzip_file_to(fname[0], gettempdir())
        return unzip_recursive(fname)
    else:
        return fname


def unzip_and_get_content(fname, index=-1, encode=False, encoding="latin1"):
    zf = zipfile.ZipFile(fname)
    name = zf.namelist()[index]
    logging.debug("zipped file %s", name)
    content = zf.read(name)
    zf.close()

    if encode:
        return content.decode(encoding)
    else:
        return content