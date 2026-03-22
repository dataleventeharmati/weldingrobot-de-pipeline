import pandas as pd
from weld_pipeline.report.dq import compute_dq_report

def test_dq_detects_missing():
    df = pd.DataFrame({
        "a": [1, None, 3],
        "b": [1, 2, 3]
    })

    report = compute_dq_report(df)

    assert report["missing_values"] > 0
