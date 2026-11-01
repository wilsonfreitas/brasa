"""Optional wheel-content verification. Enable with BRASA_BUILD_TEST=1."""

import os
import subprocess
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.skipif(
    os.environ.get("BRASA_BUILD_TEST") != "1",
    reason="set BRASA_BUILD_TEST=1 to build and inspect the wheel",
)
def test_wheel_ships_sql_and_no_templates(tmp_path):
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=REPO_ROOT,
        check=True,
    )
    wheels = list(tmp_path.glob("*.whl"))
    assert len(wheels) == 1, f"expected exactly one wheel, got {wheels}"

    with zipfile.ZipFile(wheels[0]) as zf:
        names = zf.namelist()

    # Templates live in template folders (brasa-templates) since 0.5.0.
    assert not any(n.endswith(".yaml") for n in names), "templates leaked into wheel"
    assert "brasa/files/sql/create-meta-db.sql" in names
    assert "brasa/files/sql/create-dataset-catalog.sql" in names
