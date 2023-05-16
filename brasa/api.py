import base64
import os
import shutil

import pandas as pd

from brasa.meta import CacheMetadata
from brasa.util import generate_checksum_from_file
from brasa.util import unzip_recursive
from brasa.templates import MarketDataTemplate, retrieve_template


def download_marketdata(template: str | MarketDataTemplate, **kwargs) -> CacheMetadata | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    fp, response = template.downloader.download(**kwargs)
    if fp is None:
        return None

    checksum = generate_checksum_from_file(fp)
    meta = CacheMetadata()
    meta.checksum = checksum
    meta.response = response
    meta.args = kwargs
    meta.template = template.id

    fname = f"downloaded.{template.downloader.format}"
    file_path = os.path.join(meta.download_folder, fname)
    fp_dest = open(file_path, "wb")
    shutil.copyfileobj(fp, fp_dest)
    fp.close()
    fp_dest.close()

    if template.downloader.format == "zip":
        filenames = unzip_recursive(file_path)
        for filename in filenames:
            fname = os.path.basename(filename)
            _file_path = os.path.join(meta.download_folder, fname)
            shutil.move(filename, _file_path)
            meta.downloaded_files.append(fname)
        os.remove(file_path)
    elif template.downloader.format == "base64":
        with open(file_path, "rb") as fp:
            fname = f"decoded.{template.downloader.decoded_format}"
            _file_path = os.path.join(meta.download_folder, fname)
            with open(_file_path, "wb") as fp_dest:
                base64.decode(fp, fp_dest)
            meta.downloaded_files.append(fname)
        os.remove(file_path)
    else:
        meta.downloaded_files.append(fname)

    return meta


def read_marketdata(template: str | MarketDataTemplate, meta: CacheMetadata, **kwargs) -> pd.DataFrame | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    df = template.reader.read(meta, **kwargs)
    return df