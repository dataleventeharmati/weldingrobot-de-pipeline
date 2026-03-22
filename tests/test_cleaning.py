import pandas as pd
from weld_pipeline.transform.cleaning import parse_and_clean_events

def test_clean_events_basic():
    df = pd.DataFrame({
        "ts": ["2026-01-01T00:00:00Z", None, "2026-01-01T00:01:00Z"],
        "cell_id": ["A", "A", "B"],
        "robot_id": ["R1", "R1", "R2"],
        "job_id": ["J1", "J2", "J3"],
        "program_id": ["P1", "P2", "P3"],
        "event_type": ["START_CYCLE", "INVALID", "END_CYCLE"],
        "error_code": [None, None, None],
    })

    cleaned = parse_and_clean_events(df)

    assert cleaned is not None
    assert len(cleaned) == 2
    assert set(cleaned["event_type"]) == {"START_CYCLE", "END_CYCLE"}
