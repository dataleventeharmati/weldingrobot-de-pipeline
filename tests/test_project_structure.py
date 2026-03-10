from __future__ import annotations

from pathlib import Path


def test_key_project_files_exist():
    expected = [
        Path("README.md"),
        Path("pyproject.toml"),
        Path(".github/workflows/ci.yml"),
        Path("src/weld_pipeline/cli.py"),
        Path("tests/test_alerts.py"),
        Path("tests/test_config_loader.py"),
    ]
    for path in expected:
        assert path.exists(), f"Missing expected file: {path}"
