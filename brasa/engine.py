
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


class Singleton(object):
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        it = object.__new__(cls)
        cls.__it__ = it
        it.init(*args, **kwds)
        return it


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
    def __init__(self, template: str) -> None:
        self.template = template
        self.timestamp = datetime.now()
        self.response = None
        self.download_checksum = None
        self.download_args = None
        self.downloaded_files = []
        self.processed_files = []
        self.extra_key = None

    def to_dict(self) -> dict:
        return {
            "download_checksum": self.download_checksum,
            "timestamp": self.timestamp,
            "response": self.response,
            "download_args": self.download_args,
            "template": self.template,
            "downloaded_files": self.downloaded_files,
            "processed_files": self.processed_files,
            "extra_key": self.extra_key
        }

    def from_dict(self, kwargs) -> None:
        for k in kwargs.keys():
            self.__dict__[k] = kwargs[k]

    @property
    def id(self) -> str:
        return generate_checksum_for_template(self.template, self.download_args, self.extra_key)


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
        self._extra_key = downloader.get("extra_key", None)

    @property
    def extra_key(self) -> str | None:
        if self._extra_key == "date":
            return datetime.now().isoformat()[:10]
        elif self._extra_key == "datetime":
            return datetime.now().isoformat()
        else:
            return None

    def download_args(self, **kwargs) -> dict:
        args = {}
        for key, val in self.args.items():
            if key in kwargs.keys():
                args[key] = kwargs[key]
            elif val is not None:
                args[key] = val
            else:
                raise ValueError(f"Missing argument {key}")
        return args

    def download(self, **kwargs) -> tuple[IO | None, dict[str, str]]:
        args = self.download_args(**kwargs)
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


class CacheManager(Singleton):
    def init(self) -> None:
        self._cache_folder = os.path.join(os.getcwd(), ".brasa-cache")
        os.makedirs(self._cache_folder, exist_ok=True)
        os.makedirs(self.cache_path(self.meta_folder), exist_ok=True)

    @property
    def cache_folder(self) -> str:
        return self._cache_folder
    
    def cache_path(self, fname: str) -> str:
        return os.path.join(self.cache_folder, fname)

    def db_folder(self, template: MarketDataTemplate) -> str | dict[str, str]:
        if template.reader.get("multi") is None:
            folder = os.path.join("db", template.id)
            os.makedirs(self.cache_path(folder), exist_ok=True)
            return folder
        else:
            db_folders = {}
            for name in template.reader.multi:
                folder = os.path.join("db", f"{template.id}-{name}")
                os.makedirs(self.cache_path(folder), exist_ok=True)
                db_folders[name] = folder
            return db_folders

    def exists(self, template_id: str, args: dict, refdate: datetime) -> bool:
        return self.has_meta(template_id, args) and os.path.isfile(self.parquet_file_path(refdate))

    def download_folder(self, template_id: str, checksum: str) -> str:
        folder = os.path.join(template_id, "raw", checksum)
        os.makedirs(self.cache_path(folder), exist_ok=True)
        return folder

    def downloaded_file_paths(self, template_id: str, checksum: str, files: list) -> list[str]:
        folder = self.download_folder(template_id, checksum)
        return [os.path.join(folder, f) for f in files]

    @property
    def meta_folder(self) -> str:
        return "meta"

    def has_meta(self, template_id: str, args: dict={}) -> bool:
        meta_id = generate_checksum_for_template(template_id, args)
        meta_file_path = self.cache_path(os.path.join(self.meta_folder, f"{meta_id}.yaml"))
        return os.path.isfile(meta_file_path)

    def load_meta(self, template_id: str, args: dict={}) -> CacheMetadata:
        meta_id = generate_checksum_for_template(template_id, args)
        meta_file_path = self.cache_path(os.path.join(self.meta_folder, f"{meta_id}.yaml"))
        with open(meta_file_path, "r") as fp:
            meta = yaml.load(fp, Loader=yaml.Loader)
        _meta = CacheMetadata(meta["template"])
        _meta.from_dict(meta)
        return _meta

    def save_meta(self, meta: CacheMetadata) -> None:
        meta_file_path = self.cache_path(os.path.join(self.meta_folder, f"{meta.id}.yaml"))
        with open(meta_file_path, "w") as fp:
            yaml.dump(meta.to_dict(), fp, indent=4)

    def parquet_file_path(self, fname_part: str) -> str:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", fname_part):
            fname = f"{fname_part}.parquet"
        else:
            fname = f"part-{fname_part}.parquet"
        if isinstance(self.db_folder, str):
            return os.path.join(self.db_folder, fname)
        else:
            return {n:os.path.join(f, fname) for n,f in self.db_folder.items()}

    def save_parquet(self, df: pd.DataFrame, fname_part: str) -> None:
        df.to_parquet(self.cache_path(self.parquet_file_path(fname_part)))

    def load_parquet(self, fname_part: str) -> pd.DataFrame:
        df = pd.read_parquet(self.cache_path(self.parquet_file_path(fname_part)))
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


def retrieve_template(template_name) -> MarketDataTemplate | None:
    dir = os.path.join(os.path.dirname(__file__), "../templates")
    sel = [f for f in os.listdir(dir) if template_name in f]
    if len(sel) == 0:
        raise ValueError(f"Invalid template {template_name}")
    else:
        template_path = os.path.join(dir, sel[0])
        return MarketDataTemplate(template_path)


def download_marketdata(meta: CacheMetadata, **kwargs) -> CacheMetadata | None:
    template = retrieve_template(meta.template)
    fp, response = template.downloader.download(**kwargs)
    meta.response = response
    meta.download_args = kwargs
    meta.extra_key = template.downloader.extra_key
    if fp is None:
        return None

    checksum = generate_checksum_from_file(fp)
    meta.download_checksum = checksum

    man = CacheManager()
    download_folder = man.download_folder(template.id, checksum)

    fname = f"downloaded.{template.downloader.format}"
    file_rel_path = os.path.join(download_folder, fname)
    fp_dest = open(man.cache_path(file_rel_path), "wb")
    shutil.copyfileobj(fp, fp_dest)
    fp.close()
    fp_dest.close()

    if template.downloader.format == "zip":
        filenames = unzip_recursive(man.cache_path(file_rel_path))
        for filename in filenames:
            fname = os.path.basename(filename)
            _file_rel_path = os.path.join(download_folder, fname)
            shutil.move(filename, man.cache_path(_file_rel_path))
            meta.downloaded_files.append(_file_rel_path)
        os.remove(man.cache_path(file_rel_path))
    elif template.downloader.format == "base64":
        with open(man.cache_path(file_rel_path), "rb") as fp:
            fname = f"decoded.{template.downloader.decoded_format}"
            _file_rel_path = os.path.join(download_folder, fname)
            with open(man.cache_path(_file_rel_path), "wb") as fp_dest:
                base64.decode(fp, fp_dest)
            meta.downloaded_files.append(_file_rel_path)
        os.remove(man.cache_path(file_rel_path))
    else:
        meta.downloaded_files.append(file_rel_path)


def read_marketdata(meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    template = retrieve_template(template)
    df = template.reader.read(meta)
    man = CacheManager()
    if isinstance(df, dict):
        pass
    else:
        if "refdate" in df:
            fname_part = df["refdate"].iloc[0].isoformat()[:10]
        elif "refdate" in meta.download_args:
            fname_part = meta.download_args["refdate"].isoformat()[:10]
        else:
            fname_part = meta.download_checksum
        man.save_parquet(df, fname_part)
    return df


def download_and_read_marketdata(template: str | MarketDataTemplate, **kwargs) -> pd.DataFrame | None:
    if isinstance(template, str):
        template = retrieve_template(template)
        if template is None:
            raise ValueError(f"Invalid template {template}")
    cache = CacheManager(template, kwargs)
    return cache.process_with_checks()



