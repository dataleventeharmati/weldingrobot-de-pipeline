import pandas as pd
from weld_pipeline.report.kpi import compute_kpis

def test_kpi_basic():
    df = pd.DataFrame({
        "cycle_time": [10, 20, 30],
        "defect": [0, 1, 0],
        "energy": [5, 6, 7]
    })

    kpis = compute_kpis(df)

    assert "avg_cycle_time" in kpis
    assert kpis["avg_cycle_time"] == 20
    assert kpis["defect_rate"] > 0
