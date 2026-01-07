import argparse
import logging
import json
from pathlib import Path
import pandas as pd

from rich.console import Console

from weld_pipeline.logging_conf import setup_logging
from weld_pipeline.generate.synthetic_factory import GenConfig, generate_synthetic, write_outputs

from weld_pipeline.io.paths import OutputPaths
from weld_pipeline.transform.cleaning import parse_and_clean_events, parse_and_clean_quality
from weld_pipeline.transform.dq import build_dq_report, report_to_dict
from weld_pipeline.report.kpi import compute_kpis
from weld_pipeline.report.alerts import (
    alert_scrap_rate,
    alert_long_downtime,
    alert_cycle_time_p95,
)
from weld_pipeline.config.loader import load_thresholds, ConfigLoadError


log = logging.getLogger(__name__)
console = Console()


def cmd_generate(args: argparse.Namespace) -> int:
    cfg = GenConfig(
        days=args.days,
        cells=args.cells,
        robots_per_cell=args.robots,
        seed=args.seed,
        out_dir=args.out_dir,
    )
    log.info("Generating synthetic data: %s", cfg)

    events, quality = generate_synthetic(cfg)
    events_path, quality_path = write_outputs(events, quality, cfg.out_dir)

    console.print("[green]OK[/green] generated files:")
    console.print(f" - {events_path}")
    console.print(f" - {quality_path}")
    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    paths = OutputPaths()
    paths.ensure()
    stamp = paths.stamp()

    events_raw = pd.read_csv(args.events)
    quality_raw = pd.read_csv(args.quality)

    events_clean = parse_and_clean_events(events_raw)
    quality_clean = parse_and_clean_quality(quality_raw)

    dq = build_dq_report(events_raw, events_clean, quality_raw, quality_clean)
    dq_dict = report_to_dict(dq)

    events_out = paths.staged_dir / f"robot_events_staged_{stamp}.csv"
    quality_out = paths.staged_dir / f"quality_checks_staged_{stamp}.csv"
    events_clean.to_csv(events_out, index=False)
    quality_clean.to_csv(quality_out, index=False)

    # timestamped DQ report
    report_path = paths.reports_dir / f"dq_report_{stamp}.json"
    report_path.write_text(json.dumps(dq_dict, indent=2), encoding="utf-8")

    # latest DQ report (idempotent)
    latest_report_path = paths.reports_dir / "dq_report_latest.json"
    latest_report_path.write_text(json.dumps(dq_dict, indent=2), encoding="utf-8")

    console.print("[green]OK[/green] transform complete:")
    console.print(f" - staged events: {events_out}")
    console.print(f" - staged quality: {quality_out}")
    console.print(f" - dq report: {report_path}")
    console.print(f" - dq latest: {latest_report_path}")
    console.print(dq_dict)

    return 0


def _max_downtime_event_seconds(events: pd.DataFrame) -> float:
    """
    Returns the maximum downtime (seconds) for a single event, computed as:
    ERROR.ts -> next RESET.ts within the same (cell_id, robot_id) stream.

    We apply sanity:
      - 0 < dt <= 3600 seconds
    If no pairs exist, returns 0.0
    """
    df = events.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts", "cell_id", "robot_id", "event_type"])

    er = df[df["event_type"].isin(["ERROR", "RESET"])].sort_values("ts")
    if er.empty:
        return 0.0

    max_dt = 0.0

    for (_, _), g in er.groupby(["cell_id", "robot_id"]):
        g = g.sort_values("ts").reset_index(drop=True)

        for i, row in g.iterrows():
            if row["event_type"] != "ERROR":
                continue

            after = g.iloc[i + 1 :]
            reset = after[after["event_type"] == "RESET"]
            if reset.empty:
                continue

            dt = (reset.iloc[0]["ts"] - row["ts"]).total_seconds()
            if 0 < dt <= 3600 and dt > max_dt:
                max_dt = float(dt)

    return max_dt


def _safe_load_thresholds() -> dict | None:
    """
    Try to load thresholds from YAML. If it fails, fall back to defaults (None).
    """
    try:
        return load_thresholds()
    except ConfigLoadError as exc:
        log.warning("Threshold config not loaded, using defaults: %s", exc)
        return None


def cmd_report_kpi(args: argparse.Namespace) -> int:
    paths = OutputPaths()
    paths.ensure()
    stamp = paths.stamp()

    events = pd.read_csv(args.events)
    quality = pd.read_csv(args.quality)

    kpis = compute_kpis(events, quality)

    thresholds = _safe_load_thresholds()

    # Alerts (scrap + longest downtime event + cycle p95)
    max_dt = _max_downtime_event_seconds(events)
    kpis["max_downtime_event_sec"] = round(float(max_dt), 1)

    p95_cycle = kpis.get("cycle_time_sec", {}).get("p95") or 0.0
    kpis["cycle_time_p95_sec"] = round(float(p95_cycle), 1)

    alerts = [
        alert_scrap_rate(kpis["scrap_rate"], thresholds=thresholds),
        alert_long_downtime(max_dt, thresholds=thresholds),
        alert_cycle_time_p95(float(p95_cycle), thresholds=thresholds),
    ]
    kpis["alerts"] = alerts

    # timestamped report
    report_path = paths.reports_dir / f"kpi_report_{stamp}.json"
    report_path.write_text(json.dumps(kpis, indent=2), encoding="utf-8")

    # latest report (idempotent)
    latest_path = paths.reports_dir / "kpi_report_latest.json"
    latest_path.write_text(json.dumps(kpis, indent=2), encoding="utf-8")

    console.print("[green]OK[/green] KPI report generated:")
    console.print(f" - {report_path}")
    console.print(f" - {latest_path}")
    console.print(kpis)

    return 0


def _pick_latest_file(dir_path: str | Path, prefix: str, suffix: str = ".csv") -> Path:
    d = Path(dir_path)
    candidates = sorted(d.glob(f"{prefix}*{suffix}"))
    if not candidates:
        raise FileNotFoundError(f"No files found in {d} with pattern: {prefix}*{suffix}")
    return candidates[-1]


def cmd_run(args: argparse.Namespace) -> int:
    """
    End-to-end run:
      1) generate raw
      2) transform -> staged + dq (timestamp + latest)
      3) report-kpi -> kpi (timestamp + latest)

    Uses the freshly generated raw files and freshly staged files.
    """
    # 1) generate
    gen_args = argparse.Namespace(
        days=args.days,
        cells=args.cells,
        robots=args.robots,
        seed=args.seed,
        out_dir=args.out_dir,
    )
    cmd_generate(gen_args)

    # pick the just generated raw files
    raw_events = _pick_latest_file(args.out_dir, "robot_events_")
    raw_quality = _pick_latest_file(args.out_dir, "quality_checks_")

    # 2) transform
    tr_args = argparse.Namespace(events=str(raw_events), quality=str(raw_quality))
    cmd_transform(tr_args)

    # pick latest staged outputs
    staged_events = _pick_latest_file("data/staged", "robot_events_staged_")
    staged_quality = _pick_latest_file("data/staged", "quality_checks_staged_")

    # 3) report-kpi
    rp_args = argparse.Namespace(events=str(staged_events), quality=str(staged_quality))
    cmd_report_kpi(rp_args)

    console.print("[green]OK[/green] run complete")
    console.print(f" - raw events: {raw_events}")
    console.print(f" - raw quality: {raw_quality}")
    console.print(f" - staged events: {staged_events}")
    console.print(f" - staged quality: {staged_quality}")
    console.print(f" - latest dq: data/reports/dq_report_latest.json")
    console.print(f" - latest kpi: data/reports/kpi_report_latest.json")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="weld_pipeline",
        description="Welding robot DE pipeline (synthetic -> staged -> curated -> reports)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="Generate synthetic raw datasets")
    g.add_argument("--days", type=int, default=7)
    g.add_argument("--cells", type=int, default=3)
    g.add_argument("--robots", type=int, default=2, help="robots per cell")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--out-dir", type=str, default="data/raw")
    g.set_defaults(func=cmd_generate)

    t = sub.add_parser("transform", help="Clean + validate raw datasets, write staged + DQ report")
    t.add_argument("--events", type=str, required=True)
    t.add_argument("--quality", type=str, required=True)
    t.set_defaults(func=cmd_transform)

    r = sub.add_parser("report-kpi", help="Compute KPI report from staged datasets")
    r.add_argument("--events", type=str, required=True)
    r.add_argument("--quality", type=str, required=True)
    r.set_defaults(func=cmd_report_kpi)

    run = sub.add_parser("run", help="End-to-end: generate -> transform -> report-kpi")
    run.add_argument("--days", type=int, default=7)
    run.add_argument("--cells", type=int, default=3)
    run.add_argument("--robots", type=int, default=2, help="robots per cell")
    run.add_argument("--seed", type=int, default=42)
    run.add_argument("--out-dir", type=str, default="data/raw")
    run.set_defaults(func=cmd_run)

    return p


def main() -> int:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
