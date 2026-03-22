import subprocess

def test_cli_runs():
    result = subprocess.run(
        ["python", "-m", "weld_pipeline.cli", "run"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
