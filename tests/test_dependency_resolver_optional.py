"""Regression tests for optional-dependency failure handling (audit Q9.1)."""

from types import SimpleNamespace

import pytest

import brasa.engine.api
import brasa.engine.dependency_resolver as dr
from brasa.engine.dependency_resolver import _run_upstream_templates
from brasa.engine.exceptions import DependencyResolutionError


class FakeGraph:
    def __init__(self, producers):
        self._producers = producers

    def get_producer(self, dataset_id):
        return self._producers.get(dataset_id)

    def get_dataset_paths(self, producer):
        return [f"/stale/{producer}"]

    def get_input_dataset_paths(self, producer):
        return []

    def get_template_type(self, producer):
        return "marketdata"


@pytest.fixture
def stale_outputs(monkeypatch):
    monkeypatch.setattr(dr, "_is_output_fresh", lambda op, inputs: False)
    monkeypatch.setattr(dr, "_touch_marker", lambda op: None)


def test_optional_upstream_exception_does_not_abandon_remaining_refs(
    monkeypatch, stale_outputs
):
    attempted = []

    def fake_process(producer):
        attempted.append(producer)
        if producer == "p1":
            raise RuntimeError("boom")
        return SimpleNamespace(success=True)

    monkeypatch.setattr(brasa.engine.api, "process_marketdata", fake_process)

    graph = FakeGraph({"input/ds1": "p1", "input/ds2": "p2"})
    _run_upstream_templates("tpl", "arg", ["ds1", "ds2"], graph, required=False)

    assert attempted == ["p1", "p2"]


def test_required_upstream_exception_raises(monkeypatch, stale_outputs):
    def fake_process(producer):
        raise RuntimeError("boom")

    monkeypatch.setattr(brasa.engine.api, "process_marketdata", fake_process)

    graph = FakeGraph({"input/ds1": "p1"})
    with pytest.raises(DependencyResolutionError):
        _run_upstream_templates("tpl", "arg", ["ds1"], graph, required=True)
