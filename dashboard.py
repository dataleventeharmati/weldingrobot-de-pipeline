from __future__ import annotations

from datetime import datetime
from pathlib import Path

import streamlit as st

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from weld_pipeline.dashboard.i18n import language_selector, t
from weld_pipeline.dashboard.pipeline_runner import (
    KPI_LATEST,
    DQ_LATEST,
    DRILLDOWN_LATEST,
    run_pipeline_steps,
    snapshot_latest_reports_to_timestamped,
)
from weld_pipeline.dashboard.views import (
    RunRef,
    build_run_list,
    cell_overall_status,
    emoji_for_status,
    fmt_dt,
    read_json,
    render_cell_wall,
    render_trends,
    render_worst_offenders,
    thresholds_from_kpi_alerts,
    pick_focus_cell_id,
)


def main() -> None:
    st.set_page_config(page_title="Welding Robot KPI Dashboard", layout="wide")

    # Sidebar: language
    with st.sidebar:
        language_selector()
        st.divider()

    st.title(t("app_title"))
    st.caption(t("app_caption"))

    # Sidebar: report selection + controls
    with st.sidebar:
        st.header(t("sidebar_report_view"))

        runs = build_run_list(KPI_LATEST, DQ_LATEST)
        run_labels = [r.label for r in runs]
        selected_label = st.selectbox(t("select_run"), run_labels, index=0)
        selected_run: RunRef = next((r for r in runs if r.label == selected_label), runs[0])

        st.caption(t("selected_kpi"))
        st.code(str(selected_run.kpi_path) if selected_run.kpi_path else t("na"), language="text")

        st.caption(t("selected_dq"))
        if selected_run.dq_path and selected_run.dq_path.exists():
            st.code(str(selected_run.dq_path), language="text")
        else:
            st.code(t("fallback_dq"), language="text")

        st.caption(t("selected_dd"))
        st.code(str(DRILLDOWN_LATEST) if DRILLDOWN_LATEST.exists() else t("dd_missing_hint"), language="text")

        st.divider()
        st.subheader(t("factory_overview"))
        show_factory_wall = st.checkbox(t("factory_wall"), value=True)
        wall_cols = st.slider(t("wall_columns"), min_value=2, max_value=6, value=4, step=1)
        auto_focus = st.checkbox(t("auto_focus"), value=True)

        st.divider()
        st.subheader("📈 " + t("trends"))
        show_trends = st.checkbox(t("show_trends"), value=True)
        max_runs = st.slider(t("trend_window"), min_value=5, max_value=200, value=30, step=5)

        st.divider()
        st.subheader("⚙️ " + t("demo_run"))
        st.caption(t("demo_caption"))

        save_timestamped = st.checkbox("💾 " + t("save_ts"), value=True)
        with_drilldown = st.checkbox("🧩 " + t("with_dd"), value=True)

        days = st.slider(t("days"), min_value=1, max_value=30, value=7, step=1)
        cells = st.slider(t("cells"), min_value=1, max_value=10, value=3, step=1)
        robots = st.slider(t("robots_per_cell"), min_value=1, max_value=6, value=2, step=1)

        random_seed = st.checkbox(t("random_seed"), value=True)
        if random_seed:
            seed = int(datetime.now().timestamp())
        else:
            seed = st.number_input(t("seed"), min_value=1, max_value=2_000_000_000, value=42, step=1)

        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            run_now = st.button("🧪 " + t("run_now"), type="primary")
        with col_btn2:
            if st.button("🔄 " + t("refresh")):
                st.rerun()

        kpi_before = KPI_LATEST.stat().st_mtime if KPI_LATEST.exists() else None
        dq_before = DQ_LATEST.stat().st_mtime if DQ_LATEST.exists() else None
        dd_before = DRILLDOWN_LATEST.stat().st_mtime if DRILLDOWN_LATEST.exists() else None
        st.caption(f"{t('before')}: KPI={fmt_dt(kpi_before)} | DQ={fmt_dt(dq_before)} | DD={fmt_dt(dd_before)}")

        if run_now:
            with st.spinner(t("pipeline_running") + (" → report-drilldown" if with_drilldown else "") + ")"):
                rc, out = run_pipeline_steps(
                    days=days,
                    cells=cells,
                    robots=robots,
                    seed=int(seed),
                    with_drilldown=with_drilldown,
                )

            kpi_after = KPI_LATEST.stat().st_mtime if KPI_LATEST.exists() else None
            dq_after = DQ_LATEST.stat().st_mtime if DQ_LATEST.exists() else None
            dd_after = DRILLDOWN_LATEST.stat().st_mtime if DRILLDOWN_LATEST.exists() else None

            if rc == 0:
                kpi_ts = dq_ts = dd_ts = None
                if save_timestamped:
                    kpi_ts, dq_ts, dd_ts = snapshot_latest_reports_to_timestamped(save_drilldown=with_drilldown)

                msg = f"{t('after')}: KPI={fmt_dt(kpi_after)} | DQ={fmt_dt(dq_after)}"
                if with_drilldown:
                    msg += f" | DD={fmt_dt(dd_after)}"

                if save_timestamped and (kpi_ts or dq_ts or dd_ts):
                    extra = []
                    if kpi_ts:
                        extra.append(f"KPI {t('saved')}: {kpi_ts.name}")
                    if dq_ts:
                        extra.append(f"DQ {t('saved')}: {dq_ts.name}")
                    if dd_ts:
                        extra.append(f"DD {t('saved')}: {dd_ts.name}")
                    msg += "  ✅ " + " | ".join(extra)

                st.success(msg)
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"Error (return code: {rc}).")
                with st.expander(t("last_run_log")):
                    st.code(out or "", language="text")

    # Resolve selected paths
    kpi_path = selected_run.kpi_path if selected_run.kpi_path and selected_run.kpi_path.exists() else KPI_LATEST
    dq_path = selected_run.dq_path if selected_run.dq_path and selected_run.dq_path.exists() else DQ_LATEST

    kpi = read_json(kpi_path) if kpi_path else None
    dq = read_json(dq_path) if dq_path else None
    drill = read_json(DRILLDOWN_LATEST) if DRILLDOWN_LATEST.exists() else None

    if kpi is None:
        st.error(f"{t('cannot_read_kpi')} {kpi_path}\n\n{t('run_cli_hint')}")
        st.stop()

    # Top bar
    kpi_mtime = kpi_path.stat().st_mtime if kpi_path and kpi_path.exists() else None
    dq_mtime = dq_path.stat().st_mtime if dq_path and dq_path.exists() else None
    st.subheader(t("overview"))
    st.caption(
        f"Selected KPI: **{kpi_path.name}**  |  {t('kpi_time')}: **{fmt_dt(kpi_mtime)}**  |  {t('dq_time')}: **{fmt_dt(dq_mtime)}**"
    )
    if selected_run.label != t("latest_label"):
        st.info(t("note_latest"))

    st.divider()

    # Factory overview
    st.subheader(t("factory_overview"))

    if drill is None or pd is None:
        if pd is None:
            st.warning(t("pandas_missing"))
        st.info(t("no_drill_loaded"))
    else:
        per_cell = drill.get("per_cell") or []
        df_cell = pd.DataFrame(per_cell)

        if df_cell.empty:
            st.info(t("no_per_cell_in_report"))
        else:
            thrs = thresholds_from_kpi_alerts(kpi)

            df = df_cell.copy()
            for col in ["scrap_rate", "max_downtime_event_sec", "cycle_time_p95_sec", "jobs_total", "jobs_nok"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df["status"] = df.apply(lambda r: cell_overall_status(r.to_dict(), thrs), axis=1)
            df["Status"] = df["status"].apply(emoji_for_status)

            # Auto focus worst cell
            cell_ids = df["cell_id"].astype(str).unique().tolist()
            sel = st.session_state.get("sel_cell")
            if auto_focus and (sel is None or str(sel) not in cell_ids):
                focus = pick_focus_cell_id(df)
                if focus is not None:
                    st.session_state["sel_cell"] = focus

            c1, c2, c3 = st.columns(3)
            c1.metric(t("cells_count"), int(df["cell_id"].nunique()))
            c2.metric(t("alert_cells"), int((df["status"] == "ALERT").sum()))
            c3.metric(t("warning_cells"), int((df["status"] == "WARNING").sum()))
            st.caption(t("factory_hint"))

            if show_factory_wall:
                render_cell_wall(df, cols=int(wall_cols))

            with st.expander("🔎 " + t("wall_details"), expanded=False):
                show_cols = [
                    "Status",
                    "cell_id",
                    "jobs_total",
                    "jobs_nok",
                    "scrap_rate",
                    "max_downtime_event_sec",
                    "cycle_time_p95_sec",
                ]
                st.dataframe(df[show_cols], width="stretch", hide_index=True)

                ch1, ch2, ch3 = st.columns(3)
                with ch1:
                    st.markdown(f"**{t('scrap_by_cell')}**")
                    st.bar_chart(df.set_index("cell_id")[["scrap_rate"]].dropna(), height=240)
                with ch2:
                    st.markdown(f"**{t('downtime_by_cell')}**")
                    st.bar_chart(df.set_index("cell_id")[["max_downtime_event_sec"]].dropna(), height=240)
                with ch3:
                    st.markdown(f"**{t('cycle_by_cell')}**")
                    st.bar_chart(df.set_index("cell_id")[["cycle_time_p95_sec"]].dropna(), height=240)

    st.divider()

    # Trends
    if show_trends:
        render_trends(max_runs=max_runs)
        st.divider()

    # KPI cards (quick)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(t("jobs_total"), kpi.get("jobs_total"))
    c2.metric(t("jobs_nok"), kpi.get("jobs_nok"))
    c3.metric(t("scrap_rate"), kpi.get("scrap_rate"))
    c4.metric(t("max_downtime"), kpi.get("max_downtime_event_sec"))
    c5.metric(t("cycle_p95"), kpi.get("cycle_time_p95_sec"))

    st.divider()

    # Drilldown section
    st.subheader(t("drilldown"))
    if drill is None:
        st.warning(t("dd_missing"))
    else:
        counts = drill.get("counts") or {}
        gen_at = drill.get("generated_at")
        st.caption(
            f"{t('generated_at')}: **{gen_at}** | cells: **{counts.get('cells')}** | robots: **{counts.get('robots')}**"
        )

        st.subheader(t("worst_offenders"))
        render_worst_offenders(drill)

    st.divider()

    # DQ
    st.subheader(t("dq_report"))
    if dq is None:
        st.warning(f"{t('dq_missing')} {dq_path}")
    else:
        st.json(dq)


if __name__ == "__main__":
    main()
