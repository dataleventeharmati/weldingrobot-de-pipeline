import pandas as pd
from weld_pipeline.report.kpi import compute_kpis

def test_kpi_basic():
    events = pd.DataFrame({
        "ts": [
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:10Z",
            "2026-01-01T00:00:02Z",
            "2026-01-01T00:00:08Z",
        ],
        "cell_id": ["A", "A", "A", "A"],
        "robot_id": ["R1", "R1", "R1", "R1"],
        "job_id": ["J1", "J1", "J1", "J1"],
        "program_id": ["P1", "P1", "P1", "P1"],
        "event_type": ["START_CYCLE", "END_CYCLE", "ARC_ON", "ARC_OFF"],
        "error_code": [None, None, None, None],
    })

    quality = pd.DataFrame({
        "job_id": ["J1"],
        "cell_id": ["A"],
        "robot_id": ["R1"],
        "program_id": ["P1"],
        "result": ["OK"],
        "reason": [None],
        "rework_needed": [False],
    })

    kpis = compute_kpis(events, quality)

    assert "jobs_total" in kpis
    assert kpis["jobs_total"] == 1
    assert "cycle_time_sec" in kpis
    assert kpis["cycle_time_sec"]["mean"] == 10.0
