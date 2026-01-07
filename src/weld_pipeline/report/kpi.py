from __future__ import annotations

import logging
import pandas as pd

log = logging.getLogger(__name__)


def compute_kpis(events: pd.DataFrame, quality: pd.DataFrame) -> dict:
    df = events.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)

    # helper: per job timestamps
    key = ["cell_id", "robot_id", "job_id", "program_id"]

    # START->END cycle time (seconds)
    starts = df[df["event_type"] == "START_CYCLE"].groupby(key)["ts"].min()
    ends = df[df["event_type"] == "END_CYCLE"].groupby(key)["ts"].max()
    cycle = (ends - starts).dt.total_seconds().dropna()
    cycle = cycle[(cycle >= 0) & (cycle <= 3600)]  # sanity

    # ARC_ON->ARC_OFF arc time (seconds)
    arc_on = df[df["event_type"] == "ARC_ON"].groupby(key)["ts"].min()
    arc_off = df[df["event_type"] == "ARC_OFF"].groupby(key)["ts"].max()
    arc = (arc_off - arc_on).dt.total_seconds().dropna()
    arc = arc[(arc >= 0) & (arc <= 3600)]

    # quality NOK rate
    q = quality.copy()
    q["result"] = q["result"].astype("string").str.upper()
    total_jobs = int(len(q))
    nok_jobs = int((q["result"] == "NOK").sum())
    scrap_rate = (nok_jobs / total_jobs) if total_jobs else 0.0

    # errors
    errors = df[df["event_type"] == "ERROR"]["error_code"].dropna()
    top_errors = errors.value_counts().head(10).to_dict()

    result = {
        "jobs_total": total_jobs,
        "jobs_nok": nok_jobs,
        "scrap_rate": round(scrap_rate, 4),
        "cycle_time_sec": {
            "count": int(cycle.shape[0]),
            "mean": round(float(cycle.mean()), 2) if len(cycle) else None,
            "p50": round(float(cycle.quantile(0.5)), 2) if len(cycle) else None,
            "p95": round(float(cycle.quantile(0.95)), 2) if len(cycle) else None,
        },
        "arc_on_time_sec": {
            "count": int(arc.shape[0]),
            "mean": round(float(arc.mean()), 2) if len(arc) else None,
            "p50": round(float(arc.quantile(0.5)), 2) if len(arc) else None,
            "p95": round(float(arc.quantile(0.95)), 2) if len(arc) else None,
        },
        "top_error_codes": top_errors,
    }

    return result
