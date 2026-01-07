from __future__ import annotations


def _get_thresholds(thresholds: dict | None, metric: str, default_warning: float, default_alert: float) -> tuple[float, float]:
    """
    Helper to fetch warning/alert thresholds from config dict.

    Expected YAML structure:
      <metric>:
        warning_gt: ...
        alert_gt: ...
    """
    if isinstance(thresholds, dict):
        metric_cfg = thresholds.get(metric)
        if isinstance(metric_cfg, dict):
            w = metric_cfg.get("warning_gt", default_warning)
            a = metric_cfg.get("alert_gt", default_alert)
            try:
                return float(w), float(a)
            except Exception:
                return float(default_warning), float(default_alert)

    return float(default_warning), float(default_alert)


def alert_scrap_rate(scrap_rate: float, thresholds: dict | None = None) -> dict:
    """
    Alert based on scrap rate thresholds.
    Defaults:
      - WARNING if > 0.08
      - ALERT if > 0.10
    """
    warning_gt, alert_gt = _get_thresholds(thresholds, "scrap_rate", 0.08, 0.10)

    if scrap_rate > alert_gt:
        level = "ALERT"
    elif scrap_rate > warning_gt:
        level = "WARNING"
    else:
        level = "OK"

    return {
        "metric": "scrap_rate",
        "value": round(float(scrap_rate), 4),
        "level": level,
        "thresholds": {"warning_gt": warning_gt, "alert_gt": alert_gt},
    }


def alert_long_downtime(downtime_sec: float, thresholds: dict | None = None) -> dict:
    """
    Alert for a single downtime event duration.
    Defaults:
      - WARNING if downtime > 300 sec (5 minutes)
      - ALERT if downtime > 1800 sec (30 minutes)
    """
    warning_gt, alert_gt = _get_thresholds(thresholds, "downtime_event_sec", 300, 1800)

    if downtime_sec > alert_gt:
        level = "ALERT"
    elif downtime_sec > warning_gt:
        level = "WARNING"
    else:
        level = "OK"

    return {
        "metric": "downtime_event_sec",
        "value": round(float(downtime_sec), 1),
        "level": level,
        "thresholds": {"warning_gt": warning_gt, "alert_gt": alert_gt},
    }


def alert_cycle_time_p95(p95_cycle_time_sec: float, thresholds: dict | None = None) -> dict:
    """
    Alert for p95 cycle time.
    Defaults:
      - WARNING if p95 > 120 sec
      - ALERT if p95 > 150 sec
    """
    warning_gt, alert_gt = _get_thresholds(thresholds, "cycle_time_p95_sec", 120, 150)

    if p95_cycle_time_sec > alert_gt:
        level = "ALERT"
    elif p95_cycle_time_sec > warning_gt:
        level = "WARNING"
    else:
        level = "OK"

    return {
        "metric": "cycle_time_p95_sec",
        "value": round(float(p95_cycle_time_sec), 1),
        "level": level,
        "thresholds": {"warning_gt": warning_gt, "alert_gt": alert_gt},
    }
