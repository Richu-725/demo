#!/usr/bin/env bash
# Frontend smoke test for COMP5339 A2.
# Usage: ./test_comp5339a2_frontend.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
PORT="${FRONTEND_TEST_PORT:-8625}"
DURATION="${FRONTEND_TEST_DURATION:-5}"

# Allow override; otherwise prefer conda/python interpreters on the current platform.
if [[ -n "${FRONTEND_PYTHON:-}" ]]; then
    PYTHON_CMD=("${FRONTEND_PYTHON}")
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD=(python)
elif command -v conda >/dev/null 2>&1; then
    CONDA_ENV="${CONDA_DEFAULT_ENV:-COMP5339A2}"
    PYTHON_CMD=(conda run -n "${CONDA_ENV}" python)
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
elif command -v python.exe >/dev/null 2>&1; then
    PYTHON_CMD=(python.exe)
else
    echo "python executable not found. Set FRONTEND_PYTHON or activate the COMP5339A2 environment first." >&2
    exit 1
fi
echo "Using interpreter: ${PYTHON_CMD[*]}"

API_KEY_FILE="${PROJECT_ROOT}/API_key.txt"
if [[ -z "${OE_API_KEY:-}" ]]; then
    if [[ -f "${API_KEY_FILE}" ]]; then
        OE_API_KEY="$(awk -F': ' '/API KEY/ {print $2; exit}' "${API_KEY_FILE}" | tr -d '\r')"
        export OE_API_KEY
        echo "Loaded OE_API_KEY from ${API_KEY_FILE}"
    else
        echo "OE_API_KEY not set and API_key.txt missing; cannot run frontend test." >&2
        exit 1
    fi
else
    echo "Using OE_API_KEY already present in the environment."
fi

"${PYTHON_CMD[@]}" - <<'PY'
import importlib.util
for module in ("streamlit", "pydeck"):
    if importlib.util.find_spec(module) is None:
        raise SystemExit(f"Required module '{module}' is not installed.")
PY

export STREAMLIT_BROWSER_GATHERUSAGESTATS="false"
export MQTT_HOST="${MQTT_HOST:-test.mosquitto.org}"
export MQTT_PORT="${MQTT_PORT:-1883}"

TARGET_PATH="${PROJECT_ROOT}/a2_frontend.py"
LOWER_CMD="$(printf '%s' "${PYTHON_CMD[0]}" | tr '[:upper:]' '[:lower:]')"
if [[ "${LOWER_CMD}" == *"python.exe" ]]; then
    TARGET_PATH="$(wslpath -w "${TARGET_PATH}")"
fi

echo "Starting Streamlit frontend on port ${PORT} for ~${DURATION}s..."
"${PYTHON_CMD[@]}" -m streamlit run "${TARGET_PATH}" \
    --server.headless true \
    --server.port "${PORT}" \
    --browser.gatherUsageStats false &
STREAMLIT_PID=$!

cleanup() {
    if ps -p "${STREAMLIT_PID}" >/dev/null 2>&1; then
        kill "${STREAMLIT_PID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

sleep "${DURATION}"
cleanup
wait "${STREAMLIT_PID}" 2>/dev/null || true
echo "Frontend smoke test completed."
