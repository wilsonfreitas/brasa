"""Pipeline executors must not mask typed domain exceptions (audit Q5.5)."""

import pytest

from brasa.engine.exceptions import InvalidContentException, NoDataException
from brasa.engine.pipeline.etl_executor import ETLPipeline
from brasa.engine.pipeline.executor import ReaderPipeline


class RaisingStep:
    name = "raising-step"

    def __init__(self, exc):
        self._exc = exc

    def execute(self, data, context):
        raise self._exc


def test_reader_pipeline_reraises_domain_exceptions():
    pipeline = ReaderPipeline([RaisingStep(InvalidContentException("empty"))])
    with pytest.raises(InvalidContentException):
        pipeline.execute(meta=None, reader_config={}, fields=None)


def test_reader_pipeline_wraps_unknown_exceptions():
    pipeline = ReaderPipeline([RaisingStep(ValueError("boom"))])
    with pytest.raises(RuntimeError, match="raising-step"):
        pipeline.execute(meta=None, reader_config={}, fields=None)


def test_etl_pipeline_reraises_domain_exceptions():
    pipeline = ETLPipeline([RaisingStep(NoDataException("no data"))])
    with pytest.raises(NoDataException):
        pipeline.execute(template_id="t", writer=None)


def test_etl_pipeline_wraps_unknown_exceptions():
    pipeline = ETLPipeline([RaisingStep(ValueError("boom"))])
    with pytest.raises(RuntimeError, match="raising-step"):
        pipeline.execute(template_id="t", writer=None)
