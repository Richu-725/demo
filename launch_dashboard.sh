#!/usr/bin/env bash
# Launch the Streamlit frontend with the COMP5339A2 environment primed.
# Usage:
#   ./launch_dashboard.sh
#   STREAMLIT_PORT=8631 ./launch_dashboard.sh

set -euo pipefail

ENV_NAME="COMP5339A2"
PYTHON_RUNNER=(conda run -n "${ENV_NAME}" python)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
SETUP_SCRIPT="${PROJECT_ROOT}/setup_comp5339a2.sh"

if [[ ! -f "${SETUP_SCRIPT}" ]]; then
    echo "Setup script not found at ${SETUP_SCRIPT}" >&2
    exit 1
fi

# Prepare the environment (creates/updates conda env and exports cache locations).
if [[ "${SKIP_SETUP:-0}" -ne 1 ]]; then
    source "${SETUP_SCRIPT}"
fi

export OE_API_KEY="${OE_API_KEY:-$(awk -F': ' '/API KEY/ {print $2; exit}' "${PROJECT_ROOT}/API_key.txt" 2>/dev/null | tr -d '\r')}"
if [[ -z "${OE_API_KEY:-}" ]]; then
    echo "Warning: OE_API_KEY not detected; frontend will only show seeded metadata." >&2
fi

export MQTT_HOST="${MQTT_HOST:-test.mosquitto.org}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export FRONTEND_CACHE_DIR="${FRONTEND_CACHE_DIR:-${PROJECT_ROOT}/data/cache}"

STREAMLIT_PORT="${STREAMLIT_PORT:-8631}"
STREAMLIT_HEADLESS="${STREAMLIT_HEADLESS:-true}"

echo "Launching Streamlit on port ${STREAMLIT_PORT}..."
"${PYTHON_RUNNER[@]}" -m streamlit run "${PROJECT_ROOT}/a2_frontend.py" \
    --server.headless "${STREAMLIT_HEADLESS}" \
    --server.port "${STREAMLIT_PORT}" \
    --browser.gatherUsageStats false
