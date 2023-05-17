
import base64
from datetime import datetime
import os
import re
import shutil
from typing import IO, Callable

import pandas as pd
import yaml
import regexparser

from brasa.util import generate_checksum_for_template, generate_checksum_from_file, unzip_recursive


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


class CacheMetadata:
    def __init__(self) -> None:
        self.checksum = None
        self.timestamp = datetime.now()
        self.response = None
        self.args = None
        self.template = None
        self.downloaded_files = []

    def to_dict(self) -> dict:
        return {
            "checksum": self.checksum,
            "timestamp": self.timestamp,
            "response": self.response,
            "args": self.args,
            "template": self.template,
            "downloaded_files": self.downloaded_files,
        }

    @property
    def download_folder(self) -> str:
        folder = os.path.join(os.getcwd(), ".brasa-cache", self.template, "raw", self.checksum)
        os.makedirs(folder, exist_ok=True)
        return folder

    @property
    def downloaded_file_paths(self) -> list[str]:
        return [os.path.join(self.download_folder, f) for f in self.downloaded_files]

    def from_dict(self, kwargs) -> None:
        for k in kwargs.keys():
            self.__dict__[k] = kwargs[k]


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

    def read(self, meta: CacheMetadata, **kwargs) -> pd.DataFrame:
        df = self.read_function(self, meta, **kwargs)
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


class CacheManager:
    def __init__(self, template: MarketDataTemplate, args: dict) -> None:
        self.template = template
        self.args = args
        self.cache_folder = os.path.join(os.getcwd(), ".brasa-cache")
        os.makedirs(self.cache_folder, exist_ok=True)
        self.meta_folder = os.path.join(self.cache_folder, "meta")
        os.makedirs(self.meta_folder, exist_ok=True)
        self.db_folder = os.path.join(self.cache_folder, "db", template.id)
        os.makedirs(self.db_folder, exist_ok=True)

        hash = generate_checksum_for_template(template.id, args)
        self.meta_file_path = os.path.join(self.meta_folder, f"{hash}.yaml")

    def parquet_file_path(self, refdate: datetime) -> str:
        return os.path.join(self.db_folder, f"{refdate.isoformat()[:10]}.parquet")

    def exists(self, refdate: datetime) -> bool:
        return self.has_meta and os.path.isfile(self.parquet_file_path(refdate))

    @property
    def has_meta(self) -> bool:
        return os.path.isfile(self.meta_file_path)

    def save_meta(self, meta: CacheMetadata) -> None:
        with open(self.meta_file_path, "w") as fp:
            yaml.dump(meta.to_dict(), fp, indent=4)

    def load_meta(self) -> CacheMetadata:
        with open(self.meta_file_path, "r") as fp:
            meta = yaml.load(fp, Loader=yaml.Loader)
        _meta = CacheMetadata()
        _meta.from_dict(meta)
        return _meta

    def save_parquet(self, df: pd.DataFrame, refdate: datetime) -> None:
        df.to_parquet(self.parquet_file_path(refdate))

    def load_parquet(self, refdate: datetime) -> pd.DataFrame:
        df = pd.read_parquet(self.parquet_file_path(refdate))
        return df

    def process_with_checks(self) -> pd.DataFrame | None | dict[str, pd.DataFrame]:
        refdate = self.args["refdate"]

        if self.exists(refdate):
            df = self.load_parquet(refdate)
        else:
            if self.has_meta:
                meta = self.load_meta()
            else:
                meta = download_marketdata(self.template, **self.args)
            df = read_marketdata(self.template, meta, **self.args)
            self.save_parquet(df, refdate)
            if not self.has_meta:
                self.save_meta(meta)
        return df

    def process_without_checks(self) -> pd.DataFrame | None | dict[str, pd.DataFrame]:
        refdate = self.args["refdate"]
        meta = download_marketdata(self.template, **self.args)
        df = read_marketdata(self.template, meta)
        self.save_parquet(df, refdate)
        self.save_meta(meta)
        return df


def download_and_read_marketdata(template: str | MarketDataTemplate, **kwargs) -> pd.DataFrame | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    cache = CacheManager(template, kwargs)
    return cache.process_with_checks()



