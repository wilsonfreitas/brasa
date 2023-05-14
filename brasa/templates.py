
import base64
from datetime import datetime
import hashlib
import json
import os
import re
import shutil
from typing import IO, Callable
import pandas as pd
import yaml
import regexparser

from brasa.parsers.util import unzip_recursive


def load_function_by_name(func_name: str) -> Callable:
    module_name, func_name = func_name.rsplit(".", 1)
    module = __import__(module_name, fromlist=[func_name])
    func = getattr(module, func_name)
    return func


class TemplatePart:
    pass


class NumericParser(regexparser.NumberParser):
    def parseText(self, text: str) -> str:
        return None


class PtBRNumericParser(regexparser.TextParser):
    def parseInteger(self, text: str, match: re.Match) -> int:
        r'^-?\s*\d+$'
        return eval(text)

    def parse_number_with_thousands_ptBR(self, text: str, match: re.Match) -> float:
        r'^-?\s*(\d+\.)+\d+,\d+?$'
        text = text.replace('.', '')
        text = text.replace(',', '.')
        return eval(text)

    def parse_number_decimal_ptBR(self, text: str, match: re.Match) -> float:
        r'^-?\s*\d+,\d+?$'
        text = text.replace(',', '.')
        return eval(text)

    def parseText(self, text: str) -> str:
        return None


class FieldHandler:
    def __init__(self, handler: dict | None) -> None:
        if handler is not None:
            self.__dict__.update(handler)
        self.is_empty = handler is None
        self.parser = regexparser.GenericParser()

    def parse(self, value: str | pd.Series) -> str | int | float | datetime | pd.Series:
        if isinstance(value, str):
            return self.parser.parse(value)
        else:
            return value.apply(self.parser.parse)


class CharacterFieldHandler(FieldHandler):
    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)

    def parse(self, value: str | pd.Series) -> str | pd.Series:
        if isinstance(value, str):
            return value
        else:
            return value.astype(str)


class NumericFieldHandler(FieldHandler):
    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)
        if self.__dict__.get("format") is None:
            self.parser = NumericParser()
        elif self.format == "pt-br":
            self.parser = PtBRNumericParser()
        else:
            self.parser = NumericParser()


class DateFieldHandler(FieldHandler):
    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)

    def parse(self, value: str | pd.Series) -> datetime | pd.Series:
        def func(value):
            try:
                return datetime.strptime(value, self.format)
            except ValueError:
                return None
        if isinstance(value, str):
            return func(value)
        else:
            return value.apply(func)


class FieldHandlerFactory:
    @classmethod
    def create(cls, handler: dict | None) -> FieldHandler:
        if handler is None or handler.get("type") is None:
            return FieldHandler(handler)
        elif handler["type"] == "numeric":
            return NumericFieldHandler(handler)
        elif handler["type"].lower() == "date" or handler["type"].lower() == "posixct":
            return DateFieldHandler(handler)
        elif handler["type"] == "character":
            return CharacterFieldHandler(handler)
        else:
            return FieldHandler(handler)


class TemplateField:
    def __init__(self, **kwargs) -> None:
        self.name = kwargs["name"]
        self.description = kwargs.get("description")
        self.width = kwargs.get("width", -1)
        self.handler = FieldHandlerFactory.create(kwargs.get("handler"))

    def parse(self, value: str | pd.Series) -> str | float | datetime | pd.Series:
        return self.handler.parse(value)


class TemplateFields:
    def __init__(self, fields: list) -> None:
        self.__fields = {f["name"]:TemplateField(**f) for f in fields}
        self.names = list(self.__fields.keys())

    def __len__(self) -> int:
        return len(self.__fields)

    def __getitem__(self, key: str) -> TemplateField:
        return self.__fields[key]

    def __iter__(self):
        return iter(self.__fields.values())


class MarketDataReader:
    def __init__(self, reader: dict):
        for n in reader.keys():
            self.__dict__[n] = reader[n]
        self.encoding = reader.get("encoding", "utf-8")
        self.read_function = load_function_by_name(reader["function"])
        self.parts = None
        self.fields = None

    def set_parts(self, parts: list) -> None:
        self.parts = parts

    def set_fields(self, fields: TemplateFields) -> None:
        self.fields = fields

    def read(self, fname: IO | str, parse_fields: bool=True, **kwargs) -> pd.DataFrame:
        df = self.read_function(self, fname)
        if parse_fields:
            if self.parts is not None:
                pass
            else:
                for field in self.fields:
                    df[field.name] = field.parse(df[field.name])
        return df


class MarketDataDownloader:
    def __init__(self, downloader: dict):
        for n in downloader.keys():
            self.__dict__[n] = downloader[n]
        self.args = downloader.get("args", {})
        self.encoding = downloader.get("encoding", "utf-8")
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


class MarketDataTemplate:
    def __init__(self, template_path):
        self.template_path = template_path
        self.has_reader = False
        self.has_downloader = False
        self.has_parts = False
        self.template = self.load_template()

    def load_template(self) -> dict:
        with open(self.template_path, 'r', encoding="utf-8") as f:
            template = yaml.safe_load(f)
        for n in template.keys():
            self.__dict__[n] = template[n]
            if n == "downloader":
                self.has_downloader = True
                self.downloader = MarketDataDownloader(template[n])
            elif n == "reader":
                self.has_reader = True
                self.reader = MarketDataReader(template[n])
            elif n == "fields":
                self.fields = TemplateFields(template[n])
            elif n == "parts":
                self.has_parts = True
                self.parts = template[n]
        if self.has_parts:
            self.reader.set_parts(self.parts)
        else:
            self.reader.set_fields(self.fields)
        return template


def retrieve_template(template_name) -> MarketDataTemplate | None:
    dir = os.path.join(os.path.dirname(__file__), "../templates")
    sel = [f for f in os.listdir(dir) if template_name in f]
    if len(sel) == 0:
        return None
    else:
        template_path = os.path.join(dir, sel[0])
        return MarketDataTemplate(template_path)


def get_checksum(fp: IO) -> str:
    file_hash = hashlib.md5()
    while chunk := fp.read(8192):
        file_hash.update(chunk)
    fp.seek(0)
    return file_hash.hexdigest()


def download_marketdata(template_name: str, **kwargs) -> str | None:
    template = retrieve_template(template_name)
    if template is None:
        return None
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


def read_marketdata(template_name: str, fname: IO | str, parse_fields: bool=True, **kwargs) -> pd.DataFrame | None:
    template = retrieve_template(template_name)
    if template is None:
        return None
    df = template.reader.read(fname, parse_fields=parse_fields, **kwargs)
    return df