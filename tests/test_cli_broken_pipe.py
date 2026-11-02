"""A listing piped into a reader that stops early exits without a traceback (WIL-211)."""

import os
import subprocess
import sys
from pathlib import Path

FIXTURE_FOLDER = Path(__file__).parent / "fixtures" / "templates-folder"


def test_list_templates_into_closed_pipe_has_no_traceback(tmp_path):
    env = {
        **os.environ,
        "BRASA_DATA_PATH": str(tmp_path),
        "BRASA_TEMPLATE_PATH": str(FIXTURE_FOLDER),
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "brasa", "list-templates"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    # Close our end before brasa writes (its imports take far longer), like
    # `| head -0`: every write then fails with EPIPE.
    proc.stdout.close()
    _, stderr = proc.communicate(timeout=60)
    assert b"Traceback" not in stderr
    assert b"BrokenPipeError" not in stderr
    assert proc.returncode == 1
