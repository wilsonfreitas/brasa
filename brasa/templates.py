
from typing import IO, Callable
import yaml


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
            elif n == "reader":
                self.has_reader = True
                self.reader = load_function_by_name(template[n]["function"])
            elif n == "fields":
                pass
            elif n == "parts":
                pass
        return template


class MarketDataDownloader:
    def __init__(self, downloader: dict):
        self.url_template = downloader["url"]
        self.verify_ssl = downloader.get("verify_ssl", True)
        func = load_function_by_name(downloader["function"])
        self.download_function = func

    def download(self, **kwargs) -> IO | None:
        kwargs["verify_ssl"] = self.verify_ssl
        return self.download_function(self.url_template, **kwargs)

