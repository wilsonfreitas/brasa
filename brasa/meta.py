import os
from datetime import datetime


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