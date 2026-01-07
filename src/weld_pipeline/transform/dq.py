from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
import pandas as pd

log = logging.getLogger(__name__)

@dataclass
class DQReport:
    events_rows_in: int
    events_rows_out: int
    quality_rows_in: int
    quality_rows_out: int

    missing_ts_in_raw: int
    duplicates_removed: int

    arc_on_without_off: int
    arc_off_without_on: int
    missing_start_end_pairs: int

def build_dq_report(events_raw: pd.DataFrame, events_clean: pd.DataFrame,
                    quality_raw: pd.DataFrame, quality_clean: pd.DataFrame) -> DQReport:

    missing_ts_in_raw = int(pd.to_datetime(events_raw["ts"], errors="coerce").isna().sum())

    # rough duplicate estimate (same key as cleaning)
    key_cols = ["ts", "cell_id", "robot_id", "job_id", "program_id", "event_type", "error_code"]
    raw_dupes = int(events_raw.duplicated(subset=[c for c in key_cols if c in events_raw.columns]).sum())
    duplicates_removed = max(0, raw_dupes)  # cleaning also drops other invalid rows, but this is a good signal

    # ARC pairing checks per job (after cleaning)
    grp = events_clean.groupby(["cell_id", "robot_id", "job_id"], dropna=False)
    arc_on_wo_off = 0
    arc_off_wo_on = 0
    missing_start_end = 0

    for _, g in grp:
        types = g["event_type"].tolist()
        arc_on = types.count("ARC_ON")
        arc_off = types.count("ARC_OFF")
        if arc_on > arc_off:
            arc_on_wo_off += (arc_on - arc_off)
        elif arc_off > arc_on:
            arc_off_wo_on += (arc_off - arc_on)

        start = types.count("START_CYCLE")
        end = types.count("END_CYCLE")
        if start != end:
            missing_start_end += abs(start - end)

    return DQReport(
        events_rows_in=len(events_raw),
        events_rows_out=len(events_clean),
        quality_rows_in=len(quality_raw),
        quality_rows_out=len(quality_clean),
        missing_ts_in_raw=missing_ts_in_raw,
        duplicates_removed=duplicates_removed,
        arc_on_without_off=int(arc_on_wo_off),
        arc_off_without_on=int(arc_off_wo_on),
        missing_start_end_pairs=int(missing_start_end),
    )

def report_to_dict(r: DQReport) -> dict:
    return asdict(r)
