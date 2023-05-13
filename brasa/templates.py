
import base64
from datetime import datetime
import hashlib
import json
import os
import shutil
from typing import IO, Callable
import yaml

from brasa.parsers.util import unzip_recursive


def load_function_by_name(func_name: str) -> Callable:
    module_name, func_name = func_name.rsplit(".", 1)
    module = __import__(module_name, fromlist=[func_name])
    func = getattr(module, func_name)
    return func


class MarketDataTemplate:
    def __init__(self, template_path):
        self.template_path = template_path
        self.has_reader = False
        self.has_downloader = False
        self.template = self.load_template()

    def load_template(self) -> dict:
        with open(self.template_path, 'r', encoding="UTF8") as f:
            template = yaml.safe_load(f)
        for n in template.keys():
            self.__dict__[n] = template[n]
            if n == "downloader":
                self.has_downloader = True
                self.downloader = MarketDataDownloader(template[n])
            elif n == "reader":
                self.has_reader = True
                # self.reader = load_function_by_name(template[n]["function"])
            elif n == "fields":
                pass
            elif n == "parts":
                pass
        return template


def retrieve_template(template_name) -> MarketDataTemplate | None:
    dir = os.path.join(os.path.dirname(__file__), "../templates")
    sel = [f for f in os.listdir(dir) if template_name in f]
    if len(sel) == 0:
        return None
    else:
        template_path = os.path.join(dir, sel[0])
        return MarketDataTemplate(template_path)


class MarketDataDownloader:
    def __init__(self, downloader: dict):
        for n in downloader.keys():
            self.__dict__[n] = downloader[n]
        self.args = downloader.get("args", {})
        self.encoding = downloader.get("encoding", "UTF8")
        self.verify_ssl = downloader.get("verify_ssl", True)
        self.download_function = load_function_by_name(downloader["function"])

    def download(self, **kwargs) -> IO | None:
        args = {}
        for key, val in self.args.items():
            if key in kwargs.keys():
                args[key] = kwargs[key]
            elif val is not None:
                args[key] = val
            else:
                raise ValueError(f"Missing argument {key}")
        return self.download_function(self.url, self.verify_ssl, **args)


def get_checksum(fp: IO) -> str:
    file_hash = hashlib.md5()
    while chunk := fp.read(8192):
        file_hash.update(chunk)
    fp.seek(0)
    return file_hash.hexdigest()


def download_marketdata(template_name, **kwargs) -> str | None:
    template = retrieve_template(template_name)
    fp, response = template.downloader.download(**kwargs)
    if fp is None:
        return None
    if template.downloader.format in ("zip", "base64"):
        dest = os.path.join(os.getcwd(), ".brasa-cache", template_name, "raw")
    else:
        dest = os.path.join(os.getcwd(), ".brasa-cache", template_name, "downloads")
    os.makedirs(dest, exist_ok=True)
    
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    checksum = get_checksum(fp)
    fname = f"{timestamp_str}_{checksum}.{template.downloader.format}"
    file_path = os.path.join(dest, fname)
    fp_dest = open(file_path, "wb")
    shutil.copyfileobj(fp, fp_dest)
    fp.close()
    fp_dest.close()

    dest = os.path.join(os.getcwd(), ".brasa-cache", template_name, "downloads")
    os.makedirs(dest, exist_ok=True)
    with open(os.path.join(dest, f"{timestamp_str}_response.json"), "w") as fp:
        json.dump(dict(response.headers), fp, indent=4)
    
    if template.downloader.format == "zip":
        filenames = unzip_recursive(file_path)
        dest = os.path.join(os.getcwd(), ".brasa-cache", template_name, "downloads")
        os.makedirs(dest, exist_ok=True)
        file_path = []
        for filename in filenames:
            with open(filename, "rb") as fp:
                checksum = get_checksum(fp)
            fname = f"{timestamp_str}_{checksum}{os.path.splitext(filename)[1].lower()}"
            _file_path = os.path.join(dest, fname)
            os.rename(filename, _file_path)
            file_path.append(_file_path)
    elif template.downloader.format == "base64":
        dest = os.path.join(os.getcwd(), ".brasa-cache", template_name, "downloads")
        os.makedirs(dest, exist_ok=True)
        with open(file_path, "rb") as fp:
            checksum = get_checksum(fp)
            fname = f"{timestamp_str}_{checksum}.{template.downloader.decoded_format}"
            with open(os.path.join(dest, fname), "wb") as fp_dest:
                base64.decode(fp, fp_dest)


    if (isinstance(file_path, str) and os.path.exists(file_path)) or (isinstance(file_path, list) and all([os.path.exists(f) for f in file_path])):
        return file_path
    else:
        return None