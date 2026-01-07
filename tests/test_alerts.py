from __future__ import annotations

from weld_pipeline.report.alerts import (
    _get_thresholds,
    alert_scrap_rate,
    alert_long_downtime,
    alert_cycle_time_p95,
)


def test_get_thresholds_defaults_when_none():
    w, a = _get_thresholds(None, "scrap_rate", 0.08, 0.10)
    assert w == 0.08
    assert a == 0.10


def test_get_thresholds_from_config_ok():
    cfg = {"scrap_rate": {"warning_gt": 0.2, "alert_gt": 0.3}}
    w, a = _get_thresholds(cfg, "scrap_rate", 0.08, 0.10)
    assert w == 0.2
    assert a == 0.3


def test_get_thresholds_from_config_handles_non_numeric():
    cfg = {"scrap_rate": {"warning_gt": "bad", "alert_gt": "bad"}}
    w, a = _get_thresholds(cfg, "scrap_rate", 0.08, 0.10)
    assert w == 0.08
    assert a == 0.10


def test_alert_scrap_rate_levels_default_thresholds():
    assert alert_scrap_rate(0.05)["level"] == "OK"
    assert alert_scrap_rate(0.081)["level"] == "WARNING"
    assert alert_scrap_rate(0.101)["level"] == "ALERT"


def test_alert_scrap_rate_uses_config_thresholds():
    cfg = {"scrap_rate": {"warning_gt": 0.5, "alert_gt": 0.9}}
    assert alert_scrap_rate(0.4, thresholds=cfg)["level"] == "OK"
    assert alert_scrap_rate(0.6, thresholds=cfg)["level"] == "WARNING"
    assert alert_scrap_rate(0.95, thresholds=cfg)["level"] == "ALERT"


def test_alert_long_downtime_levels_default_thresholds():
    assert alert_long_downtime(10)["level"] == "OK"
    assert alert_long_downtime(301)["level"] == "WARNING"
    assert alert_long_downtime(1801)["level"] == "ALERT"


def test_alert_long_downtime_uses_config_thresholds():
    cfg = {"downtime_event_sec": {"warning_gt": 30, "alert_gt": 60}}
    assert alert_long_downtime(25, thresholds=cfg)["level"] == "OK"
    assert alert_long_downtime(31, thresholds=cfg)["level"] == "WARNING"
    assert alert_long_downtime(61, thresholds=cfg)["level"] == "ALERT"


def test_alert_cycle_time_p95_levels_default_thresholds():
    assert alert_cycle_time_p95(50)["level"] == "OK"
    assert alert_cycle_time_p95(121)["level"] == "WARNING"
    assert alert_cycle_time_p95(151)["level"] == "ALERT"


def test_alert_cycle_time_p95_uses_config_thresholds():
    cfg = {"cycle_time_p95_sec": {"warning_gt": 10, "alert_gt": 20}}
    assert alert_cycle_time_p95(9, thresholds=cfg)["level"] == "OK"
    assert alert_cycle_time_p95(11, thresholds=cfg)["level"] == "WARNING"
    assert alert_cycle_time_p95(21, thresholds=cfg)["level"] == "ALERT"


def test_alert_payload_contains_thresholds_and_metric_name():
    a = alert_scrap_rate(0.05)
    assert a["metric"] == "scrap_rate"
    assert "thresholds" in a
    assert "warning_gt" in a["thresholds"]
    assert "alert_gt" in a["thresholds"]
