# WeldingRobot-DE-Pipeline

End-to-end **Data Engineering demo project** simulating an industrial **welding robot factory**.  
The project demonstrates how raw machine events can be transformed into **KPIs, alerts, drilldowns, and a management dashboard**.

Focus:
- Data Engineering pipelines
- Data Quality (DQ)
- KPI reporting and alerting
- Drilldown analytics
- Interactive dashboard
- One-command demo mode

---

## What this project demonstrates

This repository is a **portfolio-grade Data Engineering showcase**.

Key concepts:
- Synthetic industrial data generation (robots, cells, events, quality checks)
- Raw → staged → report data layers
- Data Quality reporting
- KPI computation with threshold-based alerts
- Drilldown analytics (cell / robot level)
- Modular Python CLI pipeline
- Interactive Streamlit dashboard
- Multi-language UI (EN / DE / NL / FR / HU)
- Production-style project structure (src/, CI, pytest)

---

## Domain model – Welding robot factory

- Cells: production cells in a factory
- Robots: welding robots per cell
- Events: START, END, ERROR, RESET
- Quality checks: OK / NOK weld results

Derived metrics:
- Scrap rate
- Downtime events
- Cycle time percentiles
- Error code frequencies
- Cell and robot performance indicators

---

## Architecture overview

Project structure:

data/
- raw/        synthetic raw CSV files
- staged/     cleaned and validated CSV files
- reports/
  - kpi_report_*.json
  - dq_report_*.json
  - drilldown_report_*.json

src/weld_pipeline/
- generate/   synthetic data factory
- transform/  cleaning and data quality
- report/     KPI, alerts, drilldown
- dashboard/  dashboard modules (i18n, views, runner)
- cli.py      CLI entrypoint

---

## One-command demo (recommended)

Run a complete demo pipeline and start the dashboard:

./scripts/demo_run.sh

What this does:
1. Generate synthetic welding robot data
2. Transform raw → staged datasets
3. Create KPI and DQ reports
4. Generate drilldown analytics
5. Start the Streamlit dashboard

Dashboard URL:
http://localhost:8501

Optional clean start:

./scripts/demo_clean.sh
./scripts/demo_run.sh

---

## Dashboard features

KPI overview:
- Jobs total and NOK
- Scrap rate
- Max downtime event
- Cycle time p95
- Threshold-based alerts

Alerting:
- OK / WARNING / ALERT levels
- Thresholds loaded from configuration
- Visual gauges

Factory Wall:
- Visual cell tiles
- Status coloring (OK / WARNING / ALERT)
- Auto-focus on worst-performing cell

Drilldown analytics:
- Per-cell and per-robot KPIs
- Worst offenders (scrap, downtime, cycle)
- Error code analysis

Trends:
- KPI evolution across historical runs
- Time-series visualization

Multi-language UI:
- English
- German
- Dutch
- French
- Hungarian

---

## CLI usage (advanced)

Generate raw data:

python -m weld_pipeline.cli generate --days 7 --cells 3 --robots 2

Transform raw → staged + DQ:

python -m weld_pipeline.cli transform \
  --events data/raw/robot_events_*.csv \
  --quality data/raw/quality_checks_*.csv

KPI report:

python -m weld_pipeline.cli report-kpi \
  --events data/staged/robot_events_staged_*.csv \
  --quality data/staged/quality_checks_staged_*.csv

Drilldown report:

python -m weld_pipeline.cli report-drilldown \
  --events "data/staged/robot_events_staged_*.csv" \
  --quality "data/staged/quality_checks_staged_*.csv"

End-to-end run:

python -m weld_pipeline.cli run --days 7 --cells 3 --robots 2

---

## Testing and CI

- Unit tests with pytest
- CI via GitHub Actions
- Deterministic seeds for reproducibility

Run tests locally:

pytest

---

## Why this project exists

This project demonstrates:
- Realistic industrial data modeling
- Practical Data Engineering principles
- Data Quality as a first-class concern
- KPI-driven decision support
- Explainable, demo-ready systems

Suitable for:
- Data Engineer interviews
- Portfolio reviews
- Demo presentations
- Technical deep-dives

---

## Interview talking points

- Raw vs staged vs report layers
- Data quality checks and metrics
- KPI thresholds and alert semantics
- Drilldown vs aggregate analytics
- Idempotent pipelines
- Dashboard as a consumer, not a data source

---

## Status

Version: v1.0  
State: Stable / Frozen

The dashboard and pipeline core are intentionally frozen to preserve demo stability.

---

## License

MIT
