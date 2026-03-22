import pandas as pd
from weld_pipeline.transform.clean import clean_data

def test_clean_removes_nulls():
    df = pd.DataFrame({
        "cycle_time": [10, None, 20],
        "defect": [0, 1, 0]
    })

    cleaned = clean_data(df)

    assert cleaned.isna().sum().sum() == 0
    assert len(cleaned) < len(df)
