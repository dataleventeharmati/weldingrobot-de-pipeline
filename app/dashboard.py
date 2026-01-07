from __future__ import annotations

import json
from pathlib import Path
import streamlit as st


KPI_LATEST = Path("data/reports/kpi_report_latest.json")
DQ_LATEST = Path("data/reports/dq_report_latest.json")


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _level_emoji(level: str) -> str:
    level = (level or "").upper()
    if level == "ALERT":
        return "🟥"
    if level == "WARNING":
        return "🟨"
    return "🟩"


def main() -> None:
    st.set_page_config(page_title="Welding Robot KPI Dashboard", layout="wide")
    st.title("Welding Robot KPI Dashboard")
    st.caption("Reads the latest pipeline outputs from data/reports/*_latest.json")

    with st.sidebar:
        st.header("Inputs")
        st.write("KPI:", str(KPI_LATEST))
        st.write("DQ:", str(DQ_LATEST))
        if st.button("Refresh"):
            st.rerun()

        st.divider()
        st.subheader("Run pipeline")
        st.code("python -m weld_pipeline.cli run --days 7 --cells 3 --robots 2", language="bash")

    kpi = _read_json(KPI_LATEST)
    dq = _read_json(DQ_LATEST)

    if kpi is None:
        st.error(
            f"Missing or unreadable KPI report: {KPI_LATEST}\n\n"
            "Run:\n"
            "python -m weld_pipeline.cli run --days 7 --cells 3 --robots 2"
        )
        st.stop()

    # --- TOP KPIs ---
    jobs_total = kpi.get("jobs_total")
    jobs_nok = kpi.get("jobs_nok")
    scrap_rate = kpi.get("scrap_rate")
    max_dt = kpi.get("max_downtime_event_sec")
    cycle_p95 = kpi.get("cycle_time_p95_sec")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Jobs total", jobs_total)
    c2.metric("Jobs NOK", jobs_nok)
    c3.metric("Scrap rate", scrap_rate)
    c4.metric("Max downtime (sec)", max_dt)
    c5.metric("Cycle p95 (sec)", cycle_p95)

    st.divider()

    # --- ALERTS ---
    st.subheader("Alerts")
    alerts = kpi.get("alerts", []) or []
    if not alerts:
        st.info("No alerts in KPI report.")
    else:
        rows = []
        for a in alerts:
            level = a.get("level", "OK")
            rows.append(
                {
                    "Status": f"{_level_emoji(level)} {level}",
                    "Metric": a.get("metric"),
                    "Value": a.get("value"),
                    "warning_gt": (a.get("thresholds") or {}).get("warning_gt"),
                    "alert_gt": (a.get("thresholds") or {}).get("alert_gt"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

    # --- KPI distributions ---
    st.subheader("KPI summary")
    ct = kpi.get("cycle_time_sec", {}) or {}
    at = kpi.get("arc_on_time_sec", {}) or {}

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Cycle time (sec)**")
        st.json(ct)
    with col_b:
        st.markdown("**Arc-on time (sec)**")
        st.json(at)

    st.divider()

    # --- TOP ERROR CODES ---
    st.subheader("Top error codes")
    top_errors = kpi.get("top_error_codes", {}) or {}
    if not top_errors:
        st.info("No error codes found.")
    else:
        err_rows = [{"error_code": k, "count": v} for k, v in top_errors.items()]
        st.dataframe(err_rows, use_container_width=True, hide_index=True)

    st.divider()

    # --- DQ REPORT ---
    st.subheader("Data Quality report (latest)")
    if dq is None:
        st.warning(f"Missing or unreadable DQ report: {DQ_LATEST}")
    else:
        st.json(dq)

    st.caption("Tip: rerun the pipeline and press Refresh to update the dashboard.")


if __name__ == "__main__":
    main()
