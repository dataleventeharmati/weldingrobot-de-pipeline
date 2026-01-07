from __future__ import annotations

import logging
import pandas as pd

log = logging.getLogger(__name__)

EVENT_TYPES_ALLOWED = {"START_CYCLE", "ARC_ON", "ARC_OFF", "END_CYCLE", "ERROR", "RESET"}

def parse_and_clean_events(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()

    # parse ts
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)

    # normalize strings
    for col in ["cell_id", "robot_id", "job_id", "program_id", "event_type", "error_code"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # keep allowed event types only (unknown -> NaN)
    df.loc[~df["event_type"].isin(list(EVENT_TYPES_ALLOWED)), "event_type"] = pd.NA

    # drop rows with missing critical fields
    before = len(df)
    df = df.dropna(subset=["ts", "cell_id", "robot_id", "job_id", "program_id", "event_type"])
    dropped = before - len(df)
    if dropped:
        log.info("Dropped %s event rows missing critical fields", dropped)

    # deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["ts", "cell_id", "robot_id", "job_id", "program_id", "event_type", "error_code"])
    if len(df) != before:
        log.info("Removed %s duplicate event rows", before - len(df))

    # sort
    df = df.sort_values(["cell_id", "robot_id", "job_id", "ts"]).reset_index(drop=True)
    return df


def parse_and_clean_quality(quality: pd.DataFrame) -> pd.DataFrame:
    df = quality.copy()

    for col in ["job_id", "cell_id", "robot_id", "program_id", "result", "reason"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    df["rework_needed"] = df["rework_needed"].astype("boolean")

    # drop missing required
    before = len(df)
    df = df.dropna(subset=["job_id", "cell_id", "robot_id", "program_id", "result"])
    if len(df) != before:
        log.info("Dropped %s quality rows missing required fields", before - len(df))

    # normalize result
    df["result"] = df["result"].str.upper()
    df.loc[~df["result"].isin(["OK", "NOK"]), "result"] = pd.NA
    df = df.dropna(subset=["result"])

    df = df.drop_duplicates(subset=["job_id"])
    return df
