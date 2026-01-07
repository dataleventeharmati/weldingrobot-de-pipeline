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


def _level_badge(level: str) -> str:
    level = (level or "OK").upper()
    if level == "ALERT":
        return "🟥 ALERT"
    if level == "WARNING":
        return "🟨 WARNING"
    return "🟩 OK"


def main() -> None:
    st.set_page_config(page_title="Welding Robot KPI Dashboard", layout="wide")
    st.title("Welding Robot KPI Dashboard")
    st.caption("A dashboard a legutóbbi futás 'latest' reportjait olvassa a data/reports mappából.")

    with st.sidebar:
        st.header("Fájlok")
        st.write("KPI:", str(KPI_LATEST))
        st.write("DQ:", str(DQ_LATEST))

        st.divider()
        st.subheader("Pipeline futtatás (példa)")
        st.code("python -m weld_pipeline.cli run --days 7 --cells 3 --robots 2", language="bash")

        if st.button("🔄 Refresh"):
            st.rerun()

    kpi = _read_json(KPI_LATEST)
    dq = _read_json(DQ_LATEST)

    if kpi is None:
        st.error(
            f"Nem találom vagy nem tudom beolvasni: {KPI_LATEST}\n\n"
            "Futtasd le a pipeline-t, hogy létrejöjjenek a riportok:\n"
            "python -m weld_pipeline.cli run --days 7 --cells 3 --robots 2"
        )
        st.stop()

    # --- KPI kártyák ---
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
    c5.metric("Cycle time p95 (sec)", cycle_p95)

    st.divider()

    # --- Alerts ---
    st.subheader("Alerts")
    alerts = kpi.get("alerts", []) or []
    if not alerts:
        st.info("Nincs alert a KPI reportban.")
    else:
        rows = []
        for a in alerts:
            level = a.get("level", "OK")
            thr = a.get("thresholds") or {}
            rows.append(
                {
                    "Status": _level_badge(level),
                    "Metric": a.get("metric"),
                    "Value": a.get("value"),
                    "warning_gt": thr.get("warning_gt"),
                    "alert_gt": thr.get("alert_gt"),
                }
            )
        st.dataframe(rows, width="stretch", hide_index=True)

    # --- KPI summary JSON ---
    st.subheader("KPI summary (részletek)")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Cycle time (sec)**")
        st.json(kpi.get("cycle_time_sec", {}) or {})
    with col_b:
        st.markdown("**Arc-on time (sec)**")
        st.json(kpi.get("arc_on_time_sec", {}) or {})

    st.divider()

    # --- Top error codes ---
    st.subheader("Top error codes")
    top_errors = kpi.get("top_error_codes", {}) or {}
    if not top_errors:
        st.info("Nincs error_code a KPI reportban.")
    else:
        err_rows = [{"error_code": k, "count": v} for k, v in top_errors.items()]
        st.dataframe(err_rows, width="stretch", hide_index=True)

    st.divider()

    # --- DQ report ---
    st.subheader("Data Quality report (latest)")
    if dq is None:
        st.warning(f"Nem találom vagy nem tudom beolvasni: {DQ_LATEST}")
    else:
        st.json(dq)

    st.caption("Tipp: ha új adatokat generálsz, futtasd újra a pipeline-t, majd nyomj Refresh-t.")


if __name__ == "__main__":
    main()
