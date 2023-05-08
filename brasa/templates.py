
from typing import Callable
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
        self.verify_ssl = True
        self.template = self.load_template()

    def load_template(self) -> dict:
        with open(self.template_path, 'r', encoding="UTF8") as f:
            template = yaml.safe_load(f)
        for n in template.keys():
            if n == "reader":
                self.has_reader = True
                self.reader = load_function_by_name(template[n]["function"])
            elif n == "downloader":
                self.has_downloader = True
                self.downloader = load_function_by_name(template[n]["function"])
            elif n == "verifyssl":
                self.verify_ssl = template[n]
            elif n == "fields":
                pass
            elif n == "parts":
                pass
            else:
                self.__dict__[n] = template[n]
        return template
