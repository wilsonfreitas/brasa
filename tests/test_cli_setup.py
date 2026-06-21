import os
import subprocess
from pathlib import Path


def test_cli_setup_prints_activation_line(tmp_path):
    """`brasa setup` prints the absolute-path BRASA_DATA_PATH export line."""
    env = {**os.environ, "BRASA_DATA_PATH": str(tmp_path)}
    expected_root = Path(tmp_path).resolve()

    result = subprocess.run(
        ["uv", "run", "python", "-m", "brasa.cli", "setup"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"Brasa home ready at {expected_root}" in result.stdout
    assert f'export BRASA_DATA_PATH="{expected_root}"' in result.stdout
