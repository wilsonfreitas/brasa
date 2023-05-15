
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
        # if parse_fields:
        #     if self.parts is not None:
        #         pass
        #     else:
        #         for field in self.fields:
        #             df[field.name] = field.parse(df[field.name])
        return df


class MarketDataDownloader:
    def __init__(self, downloader: dict) -> None:
        for n in downloader.keys():
            self.__dict__[n] = downloader[n]
        self.args = downloader.get("args", {})
        self.encoding = downloader.get("encoding", "utf-8")
        self.verify_ssl = downloader.get("verify_ssl", True)
        self.download_function = load_function_by_name(downloader["function"])

    def download(self, **kwargs) -> tuple[IO | None, dict[str, str]]:
        args = {}
        for key, val in self.args.items():
            if key in kwargs.keys():
                args[key] = kwargs[key]
            elif val is not None:
                args[key] = val
            else:
                raise ValueError(f"Missing argument {key}")
        fp, response = self.download_function(self, **args)
        return fp, response


class MarketDataTemplate:
    def __init__(self, template_path) -> None:
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
        if self.has_reader:
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


def download_marketdata(template: str | MarketDataTemplate, **kwargs) -> dict | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    fp, response = template.downloader.download(**kwargs)
    if fp is None:
        return None

    checksum = get_checksum(fp)
    dest = os.path.join(os.getcwd(), ".brasa-cache", template.id, "raw", checksum)
    os.makedirs(dest, exist_ok=True)

    fname = f"downloaded.{template.downloader.format}"
    file_path = os.path.join(dest, fname)
    fp_dest = open(file_path, "wb")
    shutil.copyfileobj(fp, fp_dest)
    fp.close()
    fp_dest.close()

    meta = {
        "checksum": checksum,
        "timestamp": datetime.now(),
        "response": response,
        "args": kwargs,
        "folder": dest,
    }

    if template.downloader.format == "zip":
        filenames = unzip_recursive(file_path)
        fnames = []
        for filename in filenames:
            fname = os.path.basename(filename)
            _file_path = os.path.join(dest, fname)
            shutil.move(filename, _file_path)
            # os.rename(filename, _file_path)
            fnames.append(fname)
        meta["downloaded_files"] = fnames
        os.remove(file_path)
    elif template.downloader.format == "base64":
        with open(file_path, "rb") as fp:
            fname = f"decoded.{template.downloader.decoded_format}"
            _file_path = os.path.join(dest, fname)
            with open(_file_path, "wb") as fp_dest:
                base64.decode(fp, fp_dest)
            meta["downloaded_files"] = [fname]
        os.remove(file_path)
    else:
        meta["downloaded_files"] = [fname]

    with open(os.path.join(dest, "meta.yaml"), "w") as fp:
        yaml.dump(meta, fp, indent=4)
    
    return meta

def read_marketdata(template: str, fname: IO | str, parse_fields: bool=True, **kwargs) -> pd.DataFrame | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    df = template.reader.read(fname, parse_fields=parse_fields, **kwargs)
    return df