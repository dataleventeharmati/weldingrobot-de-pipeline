from __future__ import annotations

from pathlib import Path

import pytest

from weld_pipeline.config.loader import load_thresholds, ConfigLoadError


def test_load_thresholds_success(tmp_path: Path):
    cfg = tmp_path / "thresholds.yaml"
    cfg.write_text(
        """
scrap_rate:
  warning_gt: 0.08
  alert_gt: 0.10
downtime_event_sec:
  warning_gt: 300
  alert_gt: 1800
""".strip(),
        encoding="utf-8",
    )

    data = load_thresholds(cfg)
    assert isinstance(data, dict)
    assert data["scrap_rate"]["warning_gt"] == 0.08
    assert data["scrap_rate"]["alert_gt"] == 0.10
    assert data["downtime_event_sec"]["warning_gt"] == 300
    assert data["downtime_event_sec"]["alert_gt"] == 1800


def test_load_thresholds_missing_file_raises(tmp_path: Path):
    missing = tmp_path / "nope.yaml"
    with pytest.raises(ConfigLoadError) as exc:
        load_thresholds(missing)
    assert "Threshold config not found" in str(exc.value)


def test_load_thresholds_invalid_yaml_raises(tmp_path: Path):
    cfg = tmp_path / "bad.yaml"
    # YAML parse error (unbalanced bracket)
    cfg.write_text("scrap_rate: [1, 2", encoding="utf-8")

    with pytest.raises(ConfigLoadError) as exc:
        load_thresholds(cfg)
    assert "Failed to load threshold config" in str(exc.value)


def test_load_thresholds_non_mapping_raises(tmp_path: Path):
    cfg = tmp_path / "list.yaml"
    cfg.write_text("- 1\n- 2\n- 3\n", encoding="utf-8")

    with pytest.raises(ConfigLoadError) as exc:
        load_thresholds(cfg)
    assert "must be a YAML mapping" in str(exc.value)
