"""Custom pipeline steps loaded from a template folder's steps.py (WIL-200).

The step registry is process-global, so every test registers its own names.
"""

import os
import textwrap
from pathlib import Path

import pytest

from brasa.engine.exceptions import TemplateFolderError
from brasa.engine.pipeline import StepRegistry
from brasa.engine.template import clear_template_cache, list_templates

_STEP = """\
from brasa.engine.pipeline import PipelineStep, StepRegistry


@StepRegistry.register("{name}")
class Step(PipelineStep):
    def execute(self, data, context):
        return data.assign(source=self.get_param("value", "custom-step"))
"""

_IMPORT_TPL = """\
id: {id}
description: CSV import through a custom step
importer:
  format: csv
reader:
  pipeline:
    steps:
      - read_csv
      - {step}
      - apply_fields
fields:
  - name: refdate
    description: Reference date
    type: date
  - name: value
    description: Value
    type: numeric
  - name: source
    description: Added by the custom step
    type: string
"""


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    clear_template_cache()
    yield
    clear_template_cache()


def _folder(root: Path, step_code: str | None = None) -> Path:
    (root / "templates").mkdir(parents=True, exist_ok=True)
    if step_code is not None:
        (root / "steps.py").write_text(step_code)
    return root


def _env(monkeypatch, *roots: Path) -> None:
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", os.pathsep.join(map(str, roots)))


def test_steps_py_registers_step(tmp_path, monkeypatch):
    _env(monkeypatch, _folder(tmp_path / "ws", _STEP.format(name="wil200_basic")))
    list_templates()
    assert StepRegistry.get("wil200_basic") is not None


def test_two_folders_each_with_steps(tmp_path, monkeypatch):
    a = _folder(tmp_path / "a", _STEP.format(name="wil200_from_a"))
    b = _folder(tmp_path / "b", _STEP.format(name="wil200_from_b"))
    _env(monkeypatch, a, b)
    list_templates()
    assert StepRegistry.get("wil200_from_a") is not None
    assert StepRegistry.get("wil200_from_b") is not None


def test_steps_package_relative_import(tmp_path, monkeypatch):
    root = _folder(tmp_path / "ws")
    pkg = root / "steps"
    pkg.mkdir()
    (pkg / "helpers.py").write_text("SOURCE = 'from-package'\n")
    (pkg / "__init__.py").write_text(
        "from .helpers import SOURCE\n" + _STEP.format(name="wil200_pkg")
    )
    _env(monkeypatch, root)
    list_templates()
    assert StepRegistry.get("wil200_pkg") is not None


def test_duplicate_step_name_is_error(tmp_path, monkeypatch):
    root = _folder(tmp_path / "ws", _STEP.format(name="sql_query"))
    _env(monkeypatch, root)
    with pytest.raises(TemplateFolderError, match="Failed to import custom steps") as e:
        list_templates()
    assert str(root / "steps.py") in str(e.value)
    assert "already registered" in str(e.value)


def test_syntax_error_names_file(tmp_path, monkeypatch):
    root = _folder(tmp_path / "ws", "def broken(:\n")
    _env(monkeypatch, root)
    with pytest.raises(TemplateFolderError, match=str(root / "steps.py")):
        list_templates()


def test_steps_loaded_once(tmp_path, monkeypatch):
    root = _folder(tmp_path / "ws", _STEP.format(name="wil200_once"))
    _env(monkeypatch, root)
    list_templates()
    clear_template_cache()
    list_templates()  # a second import would re-register and raise


def test_custom_step_runs_end_to_end(tmp_path, monkeypatch):
    from brasa import import_marketdata, process_marketdata
    from brasa.queries import get_dataset

    root = _folder(tmp_path / "ws", _STEP.format(name="wil200_e2e"))
    (root / "templates" / "wil200-demo.yaml").write_text(
        _IMPORT_TPL.format(id="wil200-demo", step="wil200_e2e")
    )
    csv = tmp_path / "data.csv"
    csv.write_text(
        textwrap.dedent("""\
        refdate,value
        2026-10-30,1.5
        2026-10-31,2.5
        """)
    )
    _env(monkeypatch, root)
    assert import_marketdata("wil200-demo", path=str(csv)).success
    assert process_marketdata("wil200-demo").success
    table = get_dataset("wil200-demo").to_table()
    assert table.column("source").to_pylist() == ["custom-step", "custom-step"]
