from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

EVENT_TYPES = ["START_CYCLE", "ARC_ON", "ARC_OFF", "END_CYCLE", "ERROR", "RESET"]
QUALITY_REASONS = ["porosity", "spatter", "lack_of_fusion", "burn_through", "dimension_fail"]

@dataclass
class GenConfig:
    days: int = 7
    cells: int = 3
    robots_per_cell: int = 2
    seed: int = 42
    out_dir: str = "data/raw"

def _cell_ids(n: int) -> list[str]:
    return [f"C{idx:02d}" for idx in range(1, n + 1)]

def _robot_ids(robots_per_cell: int) -> list[str]:
    return [f"R{idx:02d}" for idx in range(1, robots_per_cell + 1)]

def _rand_error_code(rng: np.random.Generator) -> str:
    # Ipari hangulat: rövid "CDD1" + hosszabb "GLC_..." szerűek
    if rng.random() < 0.6:
        return f"CDD{rng.integers(1, 6)}"
    return f"GLC_STOERUNG_{rng.integers(10, 99)}"

def generate_synthetic(cfg: GenConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(cfg.seed)

    start = datetime.now(timezone.utc) - timedelta(days=cfg.days)
    end = datetime.now(timezone.utc)

    cells = _cell_ids(cfg.cells)
    robots = _robot_ids(cfg.robots_per_cell)

    events_rows = []
    quality_rows = []

    job_counter = 1

    # kb. ciklusok száma / nap / robot
    cycles_per_day = 60

    for day in range(cfg.days):
        day_start = start + timedelta(days=day)
        for cell in cells:
            for robot in robots:
                # napi ciklusok
                for _ in range(cycles_per_day):
                    job_id = f"JOB{job_counter:07d}"
                    job_counter += 1

                    # START_CYCLE timestamp
                    ts0 = day_start + timedelta(minutes=float(rng.integers(0, 24 * 60)))
                    cycle_time_s = int(rng.normal(90, 18))  # átlag 90s
                    cycle_time_s = max(25, min(cycle_time_s, 180))

                    arc_on_delay = int(max(1, rng.normal(8, 3)))
                    arc_on_s = int(max(5, rng.normal(45, 12)))
                    arc_on_s = max(8, min(arc_on_s, cycle_time_s - arc_on_delay - 5))

                    program_id = f"P{int(rng.integers(1, 26)):03d}"

                    # események
                    events_rows.append([ts0, cell, robot, job_id, program_id, "START_CYCLE", None])
                    events_rows.append([ts0 + timedelta(seconds=arc_on_delay), cell, robot, job_id, program_id, "ARC_ON", None])
                    events_rows.append([ts0 + timedelta(seconds=arc_on_delay + arc_on_s), cell, robot, job_id, program_id, "ARC_OFF", None])
                    events_rows.append([ts0 + timedelta(seconds=cycle_time_s), cell, robot, job_id, program_id, "END_CYCLE", None])

                    # hibák néha
                    if rng.random() < 0.06:
                        err_ts = ts0 + timedelta(seconds=int(rng.integers(5, cycle_time_s - 2)))
                        events_rows.append([err_ts, cell, robot, job_id, program_id, "ERROR", _rand_error_code(rng)])
                        # reset néha
                        if rng.random() < 0.5:
                            events_rows.append([err_ts + timedelta(seconds=int(rng.integers(5, 45))), cell, robot, job_id, program_id, "RESET", None])

                    # minőség (NOK arány)
                    nok = rng.random() < 0.08
                    quality_rows.append([
                        job_id,
                        cell,
                        robot,
                        program_id,
                        "NOK" if nok else "OK",
                        rng.choice(QUALITY_REASONS) if nok else None,
                        bool(nok and (rng.random() < 0.35)),  # rework_needed
                    ])

    events = pd.DataFrame(
        events_rows,
        columns=["ts", "cell_id", "robot_id", "job_id", "program_id", "event_type", "error_code"],
    )
    quality = pd.DataFrame(
        quality_rows,
        columns=["job_id", "cell_id", "robot_id", "program_id", "result", "reason", "rework_needed"],
    )

    # direkt belecsempészünk pár tipikus DQ problémát (később a pipeline kiszűri)
    if len(events) > 100:
        # duplikált sor
        events = pd.concat([events, events.sample(10, random_state=cfg.seed)], ignore_index=True)
        # hiányzó ts
        events.loc[events.sample(5, random_state=cfg.seed + 1).index, "ts"] = pd.NaT

    # időszűrés biztosra
    events = events[(events["ts"].isna()) | ((events["ts"] >= start) & (events["ts"] <= end))]

    return events, quality

def write_outputs(events: pd.DataFrame, quality: pd.DataFrame, out_dir: str) -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    events_path = out / f"robot_events_{stamp}.csv"
    quality_path = out / f"quality_checks_{stamp}.csv"

    events.to_csv(events_path, index=False)
    quality.to_csv(quality_path, index=False)

    log.info("Wrote %s rows -> %s", len(events), events_path)
    log.info("Wrote %s rows -> %s", len(quality), quality_path)

    return events_path, quality_path
