import pandas as pd
from weld_pipeline.transform.dq import build_dq_report

def test_dq_report_basic():
    events_raw = pd.DataFrame({
        "ts": ["2026-01-01T00:00:00Z", None, "2026-01-01T00:01:00Z"],
        "cell_id": ["A", "A", "A"],
        "robot_id": ["R1", "R1", "R1"],
        "job_id": ["J1", "J1", "J1"],
        "program_id": ["P1", "P1", "P1"],
        "event_type": ["START_CYCLE", "ARC_ON", "END_CYCLE"],
        "error_code": [None, None, None],
    })

    events_clean = pd.DataFrame({
        "ts": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"], utc=True),
        "cell_id": ["A", "A"],
        "robot_id": ["R1", "R1"],
        "job_id": ["J1", "J1"],
        "program_id": ["P1", "P1"],
        "event_type": ["START_CYCLE", "END_CYCLE"],
        "error_code": [None, None],
    })

    quality_raw = pd.DataFrame({
        "job_id": ["J1"],
        "cell_id": ["A"],
        "robot_id": ["R1"],
        "program_id": ["P1"],
        "result": ["OK"],
        "reason": [None],
        "rework_needed": [False],
    })

    quality_clean = quality_raw.copy()

    dq = build_dq_report(events_raw, events_clean, quality_raw, quality_clean)

    assert dq.events_rows_in == 3
    assert dq.events_rows_out == 2
    assert dq.missing_ts_in_raw == 1
