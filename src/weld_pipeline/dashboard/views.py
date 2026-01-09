from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import streamlit as st

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from weld_pipeline.dashboard.i18n import t

REPORTS_DIR = Path("data/reports")


# ----------------------------
# Read JSON with cache (mtime invalidation)
# ----------------------------
@st.cache_data(show_spinner=False)
def _read_json_cached(path_str: str, mtime: float | None) -> dict | None:
    _ = mtime
    path = Path(path_str)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_json(path: Path) -> dict | None:
    mtime = path.stat().st_mtime if path.exists() else None
    return _read_json_cached(str(path), mtime)


def fmt_dt(ts: float | None) -> str:
    if not ts:
        return "-"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def fmt_dt_from_dt(dt: datetime | None) -> str:
    if not dt:
        return "-"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


# ----------------------------
# Run history
# ----------------------------
@dataclass(frozen=True)
class RunRef:
    label: str
    kpi_path: Path | None
    dq_path: Path | None
    dt: datetime | None


def parse_run_dt_from_name(path: Path) -> datetime | None:
    name = path.stem
    parts = name.split("_")
    if len(parts) < 4:
        return None
    ymd = parts[2]
    hms = parts[3]
    try:
        dt = datetime.strptime(f"{ymd}_{hms}", "%Y%m%d_%H%M%S")
        return dt.replace(tzinfo=timezone.utc).astimezone()
    except Exception:
        return None


def list_timestamped_reports(prefix: str) -> list[Path]:
    files = list(REPORTS_DIR.glob(f"{prefix}_*.json"))
    files = [p for p in files if not p.name.endswith("_latest.json")]

    def _key(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except Exception:
            return 0.0

    files.sort(key=_key, reverse=True)
    return files


def match_dq_for_kpi(kpi_path: Path) -> Path | None:
    parts = kpi_path.stem.split("_")
    if len(parts) < 4:
        return None
    ts = f"{parts[2]}_{parts[3]}"
    cand = REPORTS_DIR / f"dq_report_{ts}.json"
    if cand.exists():
        return cand
    cands = sorted(REPORTS_DIR.glob(f"dq_report_{ts}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None


def build_run_list(kpi_latest: Path, dq_latest: Path) -> list[RunRef]:
    runs: list[RunRef] = []

    latest_dt = None
    try:
        latest_dt = (
            datetime.fromtimestamp(kpi_latest.stat().st_mtime, tz=timezone.utc).astimezone()
            if kpi_latest.exists()
            else None
        )
    except Exception:
        latest_dt = None

    runs.append(
        RunRef(
            label=t("latest_label"),
            kpi_path=kpi_latest if kpi_latest.exists() else None,
            dq_path=dq_latest if dq_latest.exists() else None,
            dt=latest_dt,
        )
    )

    kpi_files = list_timestamped_reports("kpi_report")
    for kp in kpi_files:
        dt = parse_run_dt_from_name(kp)
        dq = match_dq_for_kpi(kp)
        label_dt = fmt_dt_from_dt(dt)
        runs.append(RunRef(label=f"{label_dt}  —  {kp.name}", kpi_path=kp, dq_path=dq, dt=dt))

    return runs


@st.cache_data(show_spinner=False)
def load_kpi_history_for_trends(kpi_paths: list[str], mtimes: list[float | None]) -> Any:
    _ = mtimes
    rows: list[dict] = []

    def _get_alert(alerts: list[dict], metric: str) -> dict | None:
        for a in alerts or []:
            if (a.get("metric") or "") == metric:
                return a
        return None

    for p in kpi_paths:
        path = Path(p)
        k = read_json(path)
        if not k:
            continue

        dt = parse_run_dt_from_name(path)
        if dt is None:
            try:
                dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone()
            except Exception:
                dt = None

        alerts = (k.get("alerts") or []) if isinstance(k.get("alerts"), list) else []

        def _lvl(metric: str) -> str:
            a = _get_alert(alerts, metric) or {}
            return (a.get("level") or "OK").upper()

        rows.append(
            {
                "run_dt": dt,
                "jobs_total": k.get("jobs_total"),
                "jobs_nok": k.get("jobs_nok"),
                "scrap_rate": k.get("scrap_rate"),
                "max_downtime_event_sec": k.get("max_downtime_event_sec"),
                "cycle_time_p95_sec": k.get("cycle_time_p95_sec"),
                "lvl_scrap_rate": _lvl("scrap_rate"),
                "lvl_downtime": _lvl("downtime_event_sec"),
                "lvl_cycle_p95": _lvl("cycle_time_p95_sec"),
            }
        )

    rows.sort(key=lambda r: (r.get("run_dt") is None, r.get("run_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc)))

    if pd is None:
        return rows

    df = pd.DataFrame(rows)
    if "run_dt" in df.columns:
        df = df.set_index("run_dt")
    return df


# ----------------------------
# Factory / status helpers
# ----------------------------
def _get_alert(alerts: list[dict], metric: str) -> dict | None:
    for a in alerts or []:
        if (a.get("metric") or "") == metric:
            return a
    return None


def emoji_for_status(status: str) -> str:
    s = (status or "OK").upper()
    if s == "ALERT":
        return "🟥 ALERT"
    if s == "WARNING":
        return "🟨 WARNING"
    return "🟩 OK"


def thresholds_from_kpi_alerts(kpi: dict) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    alerts = (kpi.get("alerts") or []) if isinstance(kpi.get("alerts"), list) else []
    for metric in ["scrap_rate", "downtime_event_sec", "cycle_time_p95_sec"]:
        a = _get_alert(alerts, metric) or {}
        thr = a.get("thresholds") or {}
        try:
            w = float(thr.get("warning_gt"))
            al = float(thr.get("alert_gt"))
            out[metric] = {"warning_gt": w, "alert_gt": al}
        except Exception:
            continue
    return out


def _status_for_value(value: Any, thr: dict[str, float] | None) -> str:
    if value is None or thr is None:
        return "OK"
    try:
        v = float(value)
        w = float(thr["warning_gt"])
        a = float(thr["alert_gt"])
    except Exception:
        return "OK"
    if a > 0 and v >= a:
        return "ALERT"
    if w > 0 and v >= w:
        return "WARNING"
    return "OK"


def cell_overall_status(row: dict, thrs: dict[str, dict[str, float]]) -> str:
    s1 = _status_for_value(row.get("scrap_rate"), thrs.get("scrap_rate"))
    s2 = _status_for_value(row.get("max_downtime_event_sec"), thrs.get("downtime_event_sec"))
    s3 = _status_for_value(row.get("cycle_time_p95_sec"), thrs.get("cycle_time_p95_sec"))
    order = {"OK": 0, "WARNING": 1, "ALERT": 2}
    return max([s1, s2, s3], key=lambda x: order.get(x, 0))


def pick_focus_cell_id(df_cells) -> str | None:
    if pd is None or df_cells is None or df_cells.empty:
        return None

    df = df_cells.copy()
    for col in ["scrap_rate", "max_downtime_event_sec", "cycle_time_p95_sec"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    order = {"ALERT": 0, "WARNING": 1, "OK": 2}
    df["_ord"] = df["status"].map(lambda x: order.get(str(x).upper(), 9))

    for col in ["scrap_rate", "max_downtime_event_sec", "cycle_time_p95_sec"]:
        if col not in df.columns:
            df[col] = None

    df = df.sort_values(
        by=["_ord", "scrap_rate", "max_downtime_event_sec", "cycle_time_p95_sec"],
        ascending=[True, False, False, False],
        na_position="last",
    )

    if df.empty:
        return None
    try:
        return str(df.iloc[0]["cell_id"])
    except Exception:
        return None


def _inject_tile_css() -> None:
    st.markdown(
        """
        <style>
        .cell-tile {
            border-radius: 18px;
            padding: 14px 14px 10px 14px;
            border: 1px solid rgba(255,255,255,0.10);
            box-shadow: 0 6px 20px rgba(0,0,0,0.18);
            transition: transform 120ms ease, box-shadow 120ms ease;
        }
        .cell-tile:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 26px rgba(0,0,0,0.25);
        }
        .cell-hdr {
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap: 10px;
            margin-bottom: 8px;
        }
        .cell-title { font-size: 18px; font-weight: 800; }
        .cell-status { font-size: 12px; font-weight: 800; letter-spacing: 0.4px; opacity: 0.95; }
        .cell-metrics { display:grid; grid-template-columns: 1fr 1fr; gap: 6px 12px; }
        .m-k { font-size: 11px; opacity: 0.80; }
        .m-v { font-size: 16px; font-weight: 800; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _tile_bg(status: str) -> str:
    s = (status or "OK").upper()
    if s == "ALERT":
        return "linear-gradient(135deg, rgba(220,38,38,0.28), rgba(127,29,29,0.16))"
    if s == "WARNING":
        return "linear-gradient(135deg, rgba(234,179,8,0.28), rgba(113,63,18,0.16))"
    return "linear-gradient(135deg, rgba(34,197,94,0.22), rgba(20,83,45,0.14))"


def _safe_num(x: Any) -> str:
    if x is None:
        return "-"
    try:
        v = float(x)
        if abs(v) < 1 and v != 0 and v < 1.0:
            return f"{v:.3f}"
        if abs(v) < 1000:
            return f"{v:.1f}" if v % 1 else f"{int(v)}"
        return f"{v:.0f}"
    except Exception:
        return str(x)


def render_cell_wall(df_cells, cols: int = 4) -> None:
    if pd is None:
        st.warning(t("pandas_missing"))
        return

    if df_cells is None or df_cells.empty:
        st.info(t("no_per_cell_in_report"))
        return

    cols = max(2, min(int(cols), 6))
    _inject_tile_css()

    df = df_cells.copy()
    for col in ["scrap_rate", "max_downtime_event_sec", "cycle_time_p95_sec", "jobs_total", "jobs_nok"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    order = {"ALERT": 0, "WARNING": 1, "OK": 2}
    df["_ord"] = df["status"].map(lambda x: order.get(str(x).upper(), 9))
    df = df.sort_values(by=["_ord", "scrap_rate"], ascending=[True, False]).drop(columns=["_ord"])

    tiles = df.to_dict(orient="records")

    for i in range(0, len(tiles), cols):
        row = tiles[i : i + cols]
        col_objs = st.columns(cols)
        for j, tile in enumerate(row):
            with col_objs[j]:
                cell_id = tile.get("cell_id")
                status = (tile.get("status") or "OK").upper()
                bg = _tile_bg(status)

                st.markdown(
                    f"""
                    <div class="cell-tile" style="background: {bg};">
                        <div class="cell-hdr">
                            <div class="cell-title">Cell {cell_id}</div>
                            <div class="cell-status">{emoji_for_status(status)}</div>
                        </div>
                        <div class="cell-metrics">
                            <div>
                                <div class="m-k">{t("jobs_total")}</div>
                                <div class="m-v">{_safe_num(tile.get("jobs_total"))}</div>
                            </div>
                            <div>
                                <div class="m-k">{t("jobs_nok")}</div>
                                <div class="m-v">{_safe_num(tile.get("jobs_nok"))}</div>
                            </div>
                            <div>
                                <div class="m-k">{t("scrap_rate")}</div>
                                <div class="m-v">{_safe_num(tile.get("scrap_rate"))}</div>
                            </div>
                            <div>
                                <div class="m-k">{t("max_downtime")}</div>
                                <div class="m-v">{_safe_num(tile.get("max_downtime_event_sec"))}</div>
                            </div>
                            <div>
                                <div class="m-k">{t("cycle_p95")}</div>
                                <div class="m-v">{_safe_num(tile.get("cycle_time_p95_sec"))}</div>
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                key = f"open_cell__{cell_id}"
                if st.button("➡️ " + t("open_cell"), key=key, use_container_width=True):
                    st.session_state["sel_cell"] = str(cell_id)
                    st.rerun()


def render_trends(max_runs: int) -> None:
    st.subheader(t("trends"))

    kpi_ts_files = list_timestamped_reports("kpi_report")
    kpi_ts_files = kpi_ts_files[:max_runs]

    if not kpi_ts_files:
        st.info(t("no_ts_kpi"))
        return

    mtimes: list[float | None] = []
    for p in kpi_ts_files:
        try:
            mtimes.append(p.stat().st_mtime)
        except Exception:
            mtimes.append(None)

    kpi_paths = [str(p) for p in reversed(kpi_ts_files)]  # oldest -> newest
    mtimes = list(reversed(mtimes))

    hist = load_kpi_history_for_trends(kpi_paths, mtimes)

    if pd is None:
        st.warning(t("pandas_missing"))
        return

    df = hist.copy().dropna(how="all")
    if df.empty:
        st.info(t("trend_build_failed"))
        return

    cA, cB, cC = st.columns(3)
    with cA:
        st.markdown(f"**{t('scrap_rate')}**")
        st.line_chart(df[["scrap_rate"]], height=220)
    with cB:
        st.markdown(f"**{t('cycle_p95')}**")
        st.line_chart(df[["cycle_time_p95_sec"]], height=220)
    with cC:
        st.markdown(f"**{t('max_downtime')}**")
        st.line_chart(df[["max_downtime_event_sec"]], height=220)


def render_worst_offenders(drill: dict) -> None:
    worst = drill.get("worst_offenders") or {}
    if not isinstance(worst, dict) or not worst:
        st.info(t("no_alerts"))
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**{t('worst_cells')}**")
        tabs = st.tabs([t("tab_scrap"), t("tab_downtime"), t("tab_cycle")])
        keys = ["cells_by_scrap_rate", "cells_by_max_downtime", "cells_by_cycle_p95"]
        for tab, k in zip(tabs, keys):
            with tab:
                rows = worst.get(k) or []
                if pd is not None:
                    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
                else:
                    st.json(rows)

    with c2:
        st.markdown(f"**{t('worst_robots')}**")
        tabs = st.tabs([t("tab_scrap"), t("tab_downtime"), t("tab_cycle")])
        keys = ["robots_by_scrap_rate", "robots_by_max_downtime", "robots_by_cycle_p95"]
        for tab, k in zip(tabs, keys):
            with tab:
                rows = worst.get(k) or []
                if pd is not None:
                    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
                else:
                    st.json(rows)
