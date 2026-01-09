from __future__ import annotations

import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
REPORTS_DIR = Path("data/reports")

KPI_LATEST = REPORTS_DIR / "kpi_report_latest.json"
DQ_LATEST = REPORTS_DIR / "dq_report_latest.json"
DRILLDOWN_LATEST = REPORTS_DIR / "drilldown_report_latest.json"


def run_cmd(cmd: list[str]) -> tuple[int, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(Path.cwd()))
    out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return int(proc.returncode), out.strip()


def latest_file(pattern: str, base: Path) -> Path | None:
    files = list(base.glob(pattern))
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def run_pipeline_steps(days: int, cells: int, robots: int, seed: int, with_drilldown: bool) -> Tuple[int, str]:
    log_parts: list[str] = []

    cmd_gen = [
        sys.executable, "-m", "weld_pipeline.cli",
        "generate", "--days", str(days), "--cells", str(cells), "--robots", str(robots), "--seed", str(seed),
        "--out-dir", str(RAW_DIR),
    ]
    rc, out = run_cmd(cmd_gen)
    log_parts.append("=== GENERATE ===\n" + " ".join(cmd_gen) + "\n" + out)
    if rc != 0:
        return rc, "\n\n".join(log_parts)

    events_raw = latest_file("robot_events_*.csv", RAW_DIR)
    quality_raw = latest_file("quality_checks_*.csv", RAW_DIR)
    if not events_raw or not quality_raw:
        return 2, "\n\n".join(log_parts + ["❌ Missing freshly generated raw files in data/raw."])

    cmd_tr = [
        sys.executable, "-m", "weld_pipeline.cli",
        "transform", "--events", str(events_raw), "--quality", str(quality_raw),
    ]
    rc, out = run_cmd(cmd_tr)
    log_parts.append("=== TRANSFORM ===\n" + " ".join(cmd_tr) + "\n" + out)
    if rc != 0:
        return rc, "\n\n".join(log_parts)

    events_staged = latest_file("robot_events_staged_*.csv", STAGED_DIR)
    quality_staged = latest_file("quality_checks_staged_*.csv", STAGED_DIR)
    if not events_staged or not quality_staged:
        return 3, "\n\n".join(log_parts + ["❌ Missing freshly generated staged files in data/staged."])

    cmd_kpi = [
        sys.executable, "-m", "weld_pipeline.cli",
        "report-kpi", "--events", str(events_staged), "--quality", str(quality_staged),
    ]
    rc, out = run_cmd(cmd_kpi)
    log_parts.append("=== REPORT-KPI ===\n" + " ".join(cmd_kpi) + "\n" + out)
    if rc != 0:
        return rc, "\n\n".join(log_parts)

    if with_drilldown:
        cmd_dd = [
            sys.executable, "-m", "weld_pipeline.cli",
            "report-drilldown", "--events", str(events_staged), "--quality", str(quality_staged),
        ]
        rc, out = run_cmd(cmd_dd)
        log_parts.append("=== REPORT-DRILLDOWN ===\n" + " ".join(cmd_dd) + "\n" + out)
        if rc != 0:
            return rc, "\n\n".join(log_parts)

    return 0, "\n\n".join(log_parts)


def unique_timestamped_path(prefix: str, ts: str) -> Path:
    base = REPORTS_DIR / f"{prefix}_{ts}.json"
    if not base.exists():
        return base
    for i in range(1, 100):
        cand = REPORTS_DIR / f"{prefix}_{ts}_{i:02d}.json"
        if not cand.exists():
            return cand
    return REPORTS_DIR / f"{prefix}_{ts}_99.json"


def snapshot_latest_reports_to_timestamped(save_drilldown: bool) -> tuple[Path | None, Path | None, Path | None]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    kpi_out = dq_out = dd_out = None

    if KPI_LATEST.exists():
        kpi_out = unique_timestamped_path("kpi_report", ts)
        shutil.copy2(KPI_LATEST, kpi_out)

    if DQ_LATEST.exists():
        dq_out = unique_timestamped_path("dq_report", ts)
        shutil.copy2(DQ_LATEST, dq_out)

    if save_drilldown and DRILLDOWN_LATEST.exists():
        dd_out = unique_timestamped_path("drilldown_report", ts)
        shutil.copy2(DRILLDOWN_LATEST, dd_out)

    return kpi_out, dq_out, dd_out
