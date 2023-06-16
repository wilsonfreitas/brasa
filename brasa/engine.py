
import abc
import base64
from datetime import datetime
import json
import os
import re
import shutil
import sqlite3
from typing import IO, Any, Callable

import pandas as pd
import yaml
import regexparser
import duckdb

from brasa.util import generate_checksum_for_template, generate_checksum_from_file, unzip_recursive


def json_convert_from_object(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")


def json_convert_to_object(obj):
    date_pattern = r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}Z)?"
    for key, value in obj.items():
        if isinstance(value, str) and re.match(date_pattern, value):
            obj[key] = datetime.fromisoformat(value)
    return obj


def load_function_by_name(func_name: str) -> Callable:
    module_name, func_name = func_name.rsplit(".", 1)
    module = __import__(module_name, fromlist=[func_name])
    func = getattr(module, func_name)
    return func


class Singleton(abc.ABC):
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        it = object.__new__(cls)
        cls.__it__ = it
        it.init(*args, **kwds)
        return it

    @abc.abstractmethod
    def init(self, *args, **kwds):
        ...


class TemplatePart:
    pass


class NumericParser(regexparser.NumberParser):
    def parseText(self, text: str) -> str | None:
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

    def parseText(self, text: str) -> str | None:
        return None


class FieldHandler:
    def __init__(self, handler: dict | None) -> None:
        self.format = ""
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
        if self.format == "pt-br":
            self.parser = PtBRNumericParser()
        else:
            self.parser = NumericParser()


class DateFieldHandler(FieldHandler):
    def __init__(self, handler: dict | None) -> None:
        super().__init__(handler)

    def parse(self, value: str | pd.Series) -> datetime | pd.Series | None:
        def func(value) -> datetime | None:
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
        self.download_checksum = ""
        self.download_args = {}
        self.downloaded_files = []
        self.processed_files = {}
        self.extra_key = ""

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
        self.multi = reader.get("multi", {})
        self.parts = None
        self.fields = None
        self.output_filename_format = reader.get("output-filename-format", "%Y-%m-%d")

    def set_parts(self, parts: list) -> None:
        self.parts = parts

    def set_fields(self, fields: TemplateFields) -> None:
        self.fields = fields

    def read(self, meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame]:
        df = self.read_function(meta)
        return df


class MarketDataDownloader:
    def __init__(self, downloader: dict) -> None:
        self.url = None
        self.format = ""
        self.decoded_format = ""
        for n in downloader.keys():
            self.__dict__[n] = downloader[n]
        self.args = downloader.get("args", {})
        self.encoding = downloader.get("encoding", "utf-8")
        self.verify_ssl = downloader.get("verify_ssl", True)
        self.download_function = load_function_by_name(downloader["function"])
        self._extra_key = downloader.get("extra-key", None)

    @property
    def extra_key(self) -> str:
        if self._extra_key == "date":
            return datetime.now().isoformat()[:10]
        elif self._extra_key == "datetime":
            return datetime.now().isoformat()
        else:
            return ""

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

    def download(self, **kwargs) -> tuple[IO | None, Any]:
        args = self.download_args(**kwargs)
        fp, response = self.download_function(self, **args)
        return fp, response


class MarketDataTemplate:
    def __init__(self, template_path) -> None:
        self.template_path = template_path
        self.has_reader = False
        self.has_downloader = False
        self.has_parts = False
        self.id = ""
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
    _db_filename = "brasa.db"
    _meta_db_filename = "meta.db"
    _meta_folder = "meta"
    _db_folder = "db"

    def init(self) -> None:
        self._cache_folder = os.environ.get("BRASA_DATA_PATH", os.path.join(os.getcwd(), ".brasa-cache"))
        os.makedirs(self._cache_folder, exist_ok=True)
        os.makedirs(self.cache_path(self._meta_folder), exist_ok=True)
        os.makedirs(self.cache_path(self._db_folder), exist_ok=True)
        if not os.path.exists(self.cache_path(self.db_filename)):
            self.create_db()
        if not os.path.exists(self.cache_path(self.meta_db_filename)):
            self.create_meta_db()

    @property
    def cache_folder(self) -> str:
        return self._cache_folder
    
    def cache_path(self, fname: str) -> str:
        return os.path.join(self.cache_folder, fname)

    def create_db(self) -> None:
        db_conn = duckdb.connect(database=self.cache_path(self.db_filename), read_only=False)
        db_conn.commit()
        db_conn.close()

    def create_meta_db(self) -> None:
        db_conn = sqlite3.connect(database=self.cache_path(self.meta_db_filename))
        c = db_conn.cursor()
        with open(os.path.join(os.path.dirname(__file__), "..", "sql", "create-meta-db.sql")) as f:
            c.executescript(f.read())
        db_conn.commit()
        db_conn.close()

    @property
    def db_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(database=self.cache_path(self.db_filename), read_only=False)

    @property
    def meta_db_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(database=self.cache_path(self.meta_db_filename))

    @property
    def db_filename(self) -> str:
        return os.path.join(self._db_folder, self._db_filename)

    def db_folder(self, template: MarketDataTemplate | None=None) -> str | dict[str, str]:
        if template is None:
            folder = self._db_folder
        elif template.reader.multi == {}:
            folder = os.path.join(self._db_folder, template.id)
            os.makedirs(self.cache_path(folder), exist_ok=True)
        else:
            db_folders = {}
            for name,val in template.reader.multi.items():
                folder = os.path.join(self._db_folder, f"{template.id}-{val}")
                os.makedirs(self.cache_path(folder), exist_ok=True)
                db_folders[name] = folder
            folder = db_folders
        return folder

    def download_folder(self, template_id: str, checksum: str) -> str:
        folder = os.path.join("raw", template_id, checksum)
        os.makedirs(self.cache_path(folder), exist_ok=True)
        return folder

    def downloaded_file_paths(self, template_id: str, checksum: str, files: list) -> list[str]:
        folder = self.download_folder(template_id, checksum)
        return [os.path.join(folder, f) for f in files]

    @property
    def meta_db_filename(self) -> str:
        return os.path.join(self._meta_folder, self._meta_db_filename)

    @property
    def meta_folder(self) -> str:
        os.makedirs(self.cache_path(self._meta_folder), exist_ok=True)
        return self._meta_folder
    
    def meta_file_path(self, meta: CacheMetadata) -> str:
        return os.path.join(self.cache_folder, self.meta_folder, f"{meta.id}.yaml")

    def has_meta(self, meta: CacheMetadata) -> bool:
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (meta.id,))
            return len(c.fetchall()) == 1

    def load_meta(self, meta: CacheMetadata) -> None:
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (meta.id,))
            if meta_row := c.fetchall():
                meta_row = meta_row[0]
                _meta = {
                    "download_checksum": meta_row[1],
                    "timestamp": datetime.fromisoformat(meta_row[2]),
                    "response": json.loads(meta_row[3], object_hook=json_convert_to_object),
                    "download_args": json.loads(meta_row[4], object_hook=json_convert_to_object),
                    "template": meta_row[5],
                    "downloaded_files": json.loads(meta_row[6], object_hook=json_convert_to_object),
                    "processed_files": json.loads(meta_row[7], object_hook=json_convert_to_object),
                    "extra_key": meta_row[8],
                }
            meta.from_dict(_meta)

    def save_meta(self, meta: CacheMetadata) -> None:
        with self.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("select * from cache_metadata where id = ?", (meta.id,))
            if c.fetchall():
                params = (
                    meta.download_checksum,
                    meta.timestamp.isoformat(),
                    json.dumps(meta.response, default=json_convert_from_object),
                    json.dumps(meta.download_args, default=json_convert_from_object),
                    meta.template,
                    json.dumps(meta.downloaded_files, default=json_convert_from_object),
                    json.dumps(meta.processed_files, default=json_convert_from_object),
                    meta.extra_key,
                    meta.id,
                )
                c.execute("update cache_metadata set download_checksum = ?, timestamp = ?, response = ?, download_args = ?, template = ?, downloaded_files = ?, processed_files = ?, extra_key = ? where id = ?", params)
            else:
                params = (
                    meta.id,
                    meta.download_checksum,
                    meta.timestamp.isoformat(),
                    json.dumps(meta.response, default=json_convert_from_object),
                    json.dumps(meta.download_args, default=json_convert_from_object),
                    meta.template,
                    json.dumps(meta.downloaded_files, default=json_convert_from_object),
                    json.dumps(meta.processed_files, default=json_convert_from_object),
                    meta.extra_key,
                )
                c.execute("insert into cache_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?)", params)
            conn.commit()

    def remove_meta(self, meta: CacheMetadata) -> None:
        for fname in meta.downloaded_files:
            if os.path.isfile(self.cache_path(fname)):
                os.remove(self.cache_path(fname))
        for fname in meta.processed_files.values():
            if os.path.isfile(self.cache_path(fname)):
                os.remove(self.cache_path(fname))
        meta_file_path = self.meta_file_path(meta)
        if os.path.isfile(meta_file_path):
            os.remove(meta_file_path)

    def parquet_file_name(self, fname_part: str) -> str:
        if re.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", fname_part):
            fname = f"{fname_part}.parquet"
        else:
            fname = f"part-{fname_part}.parquet"
        return fname
    
    def process_with_checks(self, meta: CacheMetadata, reprocess: str | None=None) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
        if self.has_meta(meta) and (reprocess is None or reprocess == "read"):
            self.load_meta(meta)
            if len(meta.processed_files) > 0 and reprocess != "read":
                dfs = {key:pd.read_parquet(self.cache_path(fname)) for key,fname in meta.processed_files.items()}
                if len(dfs) == 1:
                    return dfs["data"]
                else:
                    return dfs
            else:
                df = read_marketdata(meta)
                self.save_meta(meta)
                return df
        else: # reprocess == "download" or reprocess == "all"
            download_marketdata(meta, **meta.download_args)
            self.save_meta(meta)
            dfs = read_marketdata(meta)
            self.save_meta(meta)
            return dfs

    def process_without_checks(self, meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
        download_marketdata(meta, **meta.download_args)
        self.save_meta(meta)
        dfs = read_marketdata(meta)
        self.save_meta(meta)
        return dfs


def retrieve_template(template_name) -> MarketDataTemplate:
    dir = os.path.join(os.path.dirname(__file__), "../templates")
    sel = [f for f in os.listdir(dir) if template_name in f]
    if len(sel) == 0:
        raise ValueError(f"Invalid template {template_name}")
    else:
        template_path = os.path.join(dir, sel[0])
        return MarketDataTemplate(template_path)


def download_marketdata(meta: CacheMetadata, **kwargs) -> CacheMetadata | None:
    template = retrieve_template(meta.template)
    meta.download_args = kwargs
    meta.extra_key = template.downloader.extra_key
    fp, response = template.downloader.download(**kwargs)
    meta.response = response
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


def get_fname_part(meta: CacheMetadata, df: pd.DataFrame) -> str:
    template = retrieve_template(meta.template)
    fmt = template.reader.output_filename_format
    if "refdate" in meta.download_args:
        fname_part = meta.download_args["refdate"].strftime(fmt)
    elif "refdate" in df:
        fname_part = df["refdate"].iloc[0].strftime(fmt)
    else:
        fname_part = meta.download_checksum
    return fname_part


def read_marketdata(meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    template = retrieve_template(meta.template)
    df = template.reader.read(meta)
    man = CacheManager()
    db_folder = man.db_folder(template)
    if isinstance(df, dict) and isinstance(db_folder, dict):
        for name,dx in df.items():
            fname_part = get_fname_part(meta, dx)
            fname = os.path.join(db_folder[name], man.parquet_file_name(fname_part))
            meta.processed_files[template.reader.multi[name]] = fname
            dx.to_parquet(man.cache_path(fname))
        df = {template.reader.multi[k]:v for k,v in df.items()}
    elif isinstance(df, pd.DataFrame) and isinstance(db_folder, str):
        fname_part = get_fname_part(meta, df)
        fname = os.path.join(db_folder, man.parquet_file_name(fname_part))
        meta.processed_files["data"] = fname
        df.to_parquet(man.cache_path(fname))
    else:
        df = None
    return df


def get_marketdata(template_name: str, reprocess: str | None=None, **kwargs) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    template = retrieve_template(template_name)
    meta = CacheMetadata(template.id)
    meta.download_args = kwargs
    meta.extra_key = template.downloader.extra_key
    cache = CacheManager()
    return cache.process_with_checks(meta, reprocess)
