import binascii
import os
import os.path
import logging
import tempfile
from typing import IO
import zipfile
from datetime import datetime, timedelta, date, timezone
import json
import bizdays
import pytz
import requests
from contextlib import contextmanager


@contextmanager
def disable_ssl_warnings():
    import warnings
    import urllib3

    with warnings.catch_warnings():
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        yield None


class SimpleDownloader:
    def __init__(self, url, verify_ssl, **kwargs):
        self.verify_ssl = verify_ssl
        self._url = url
        self.response = None

    @property
    def url(self) -> str:
        return self._url

    @property
    def status_code(self) -> int:
        return self.response.status_code

    def download(self) -> IO | None:
        with disable_ssl_warnings():
            res = requests.get(self.url, verify=self.verify_ssl)
            self.response = res

        msg = "status_code = {} url = {}".format(res.status_code, self.url)
        logg = logging.warn if res.status_code != 200 else logging.info
        logg(msg)

        if res.status_code != 200:
            return None

        temp = tempfile.TemporaryFile()
        temp.write(res.content)
        temp.seek(0)
        return temp


class DatetimeDownloader(SimpleDownloader):
    def __init__(self, url, verify_ssl, **kwargs):
        super().__init__(url, verify_ssl, **kwargs)
        self.refdate = kwargs["refdate"]

    @property
    def url(self) -> str:
        return self.refdate.strftime(self._url)


class B3URLEncodedDownloader(SimpleDownloader):
    def __init__(self, url, verify_ssl, **kwargs):
        super().__init__(url, verify_ssl, **kwargs)
        self.args = kwargs

    @property
    def url(self) -> str:
        params = json.dumps(self.args)
        params_enc = binascii.b2a_base64(bytes(params, "utf8"), newline=False).decode("utf8").strip()
        return f"{self._url}/{params_enc}"


class B3PagedURLEncodedDownloader(B3URLEncodedDownloader):
    def __init__(self, url, verify_ssl, **kwargs):
        super().__init__(url, verify_ssl, **kwargs)
        self.page = 1

    @property
    def url(self) -> str:
        self.args["pageNumber"] = self.page
        self.args["pageSize"] = 100
        return super().url

    def download(self) -> IO | None:
        fp = super().download()
        obj = json.load(fp)
        total_pages = obj["page"]["totalPages"]
        results = obj["results"]
        if len(results) == 0:
            return None
        while self.page < total_pages:
            self.page += 1
            fp = super().download()
            obj = json.load(fp)
            results.extend(obj["results"])
        data = {"results": results}
        if "header" in obj:
            data["header"] = obj["header"]
        content = json.dumps(data)
        temp = tempfile.TemporaryFile()
        temp.write(bytes(content, "utf8"))
        temp.seek(0)
        return temp


class SettlementPricesDownloader(DatetimeDownloader):
    def __init__(self, url, verify_ssl, **kwargs):
        super().__init__(url, verify_ssl, **kwargs)

    @property
    def url(self) -> str:
        return self._url

    def download(self) -> IO | None:
        body = {
            "dData1": self.refdate.strftime("%d/%m/%Y"),
        }
        with disable_ssl_warnings():
            res = requests.post(self.url, params=body, verify=self.verify_ssl)
            self.response = res

        msg = "status_code = {} url = {}".format(res.status_code, self.url)
        logg = logging.warn if res.status_code != 200 else logging.info
        logg(msg)

        if res.status_code != 200:
            return None

        temp = tempfile.TemporaryFile()
        temp.write(res.content)
        temp.seek(0)
        return temp


class B3FilesURLDownloader(DatetimeDownloader):
    def __init__(self, url, verify_ssl, **kwargs):
        super().__init__(url, verify_ssl, **kwargs)

    @property
    def url(self) -> str:
        return f'https://arquivos.b3.com.br/api/download/?token={self._response1["token"]}'

    def download(self) -> IO | None:
        res = requests.get(self.refdate.strftime(self._url))
        self.response = res
        if res.status_code != 200:
            return None
        self._response1 = res.json()
        return super().download()


class VnaAnbimaURLDownloader(SimpleDownloader):
    calendar = bizdays.Calendar.load("ANBIMA")

    def download(self, refdate=None):
        refdate = refdate or self.get_refdate()
        logging.info("refdate %s", refdate)
        url = "https://www.anbima.com.br/informacoes/vna/vna.asp"
        body = {
            "Data": refdate.strftime("%d%m%Y"),
            "escolha": "1",
            "Idioma": "PT",
            "saida": "txt",
            "Dt_Ref_Ver": refdate.strftime("%Y%m%d"),
            "Inicio": refdate.strftime("%d/%m/%Y"),
        }
        res = requests.post(url, params=body)
        msg = "status_code = {} url = {}".format(res.status_code, url)
        logg = logging.warn if res.status_code != 200 else logging.info
        logg(msg)
        if res.status_code != 200:
            return None, None, res.status_code, refdate
        status_code = res.status_code
        temp_file = tempfile.TemporaryFile()
        temp_file.write(res.content)
        temp_file.seek(0)
        f_fname = self.get_fname(None, refdate)
        logging.info(
            "Returned from download %s %s %s %s",
            f_fname,
            temp_file,
            status_code,
            refdate,
        )
        return f_fname, temp_file, status_code, refdate

    def get_refdate(self):
        offset = self.attrs.get("offset", 0)
        refdate = self.calendar.offset(self.now, offset)
        refdate = datetime(refdate.year, refdate.month, refdate.day)
        refdate = pytz.timezone("America/Sao_Paulo").localize(refdate)
        return refdate


def save_file_to_temp_folder(attrs, fname, tfile):
    fname = "/tmp/{}".format(fname)
    logging.info("saving file %s", fname)
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    with open(fname, "wb") as f:
        f.write(tfile.read())
