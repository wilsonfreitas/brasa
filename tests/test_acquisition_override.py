import io

import pytest

from brasa.engine.exceptions import DownloadException
from brasa.engine.template import MarketDataDownloader


def _make_downloader(**extra):
    spec = {"function": "brasa.downloaders.simple_download", "args": {}}
    spec.update(extra)
    return MarketDataDownloader(spec)


def test_download_uses_acquisition_function_override():
    from brasa.downloaders import simple_download

    called = {}

    def fake(md, **kw):
        called["yes"] = True
        return io.BytesIO(b"data"), {"src": "local"}

    dl = _make_downloader()
    fp, response, _retry = dl.download(acquisition_function=fake)

    assert called.get("yes") is True
    assert response == {"src": "local"}
    # shared object must not be mutated
    assert dl.download_function is simple_download


def test_download_retry_override_zero_means_no_retry():
    dl = _make_downloader(retry_attempts=5)
    attempts = {"n": 0}

    def failing(md, **kw):
        attempts["n"] += 1
        raise DownloadException("boom")

    with pytest.raises(DownloadException):
        dl.download(acquisition_function=failing, retry_attempts=0)

    assert attempts["n"] == 1
