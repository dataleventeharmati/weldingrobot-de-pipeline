#!/usr/bin/env bash
set -euo pipefail

# --- Config (tetszés szerint állítható) ---
DAYS="${DAYS:-14}"
CELLS="${CELLS:-4}"
ROBOTS="${ROBOTS:-3}"
SEED="${SEED:-1337}"

echo "=== WeldingRobot-DE-Pipeline DEMO RUN ==="
echo "days=$DAYS cells=$CELLS robots=$ROBOTS seed=$SEED"
echo

# Always run from repo root
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# venv (ha már aktív, nem csinál semmit)
if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

echo "1) generate → transform → report-kpi"
python -m weld_pipeline.cli run \
  --days "$DAYS" \
  --cells "$CELLS" \
  --robots "$ROBOTS" \
  --seed "$SEED" \
  --out-dir data/raw

echo
echo "2) report-drilldown (latest staged files)"
# FONTOS: idézőjelek, hogy a shell ne bontsa ki wildcardra
python -m weld_pipeline.cli report-drilldown \
  --events "data/staged/robot_events_staged_*.csv" \
  --quality "data/staged/quality_checks_staged_*.csv"

echo
echo "3) Start dashboard"
echo "   http://localhost:8501"
streamlit run dashboard.py
