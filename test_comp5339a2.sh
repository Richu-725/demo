#!/usr/bin/env bash
# Run backend smoke tests inside the COMP5339A2 conda environment.
# Usage: ./test_comp5339a2.sh

set -euo pipefail

ENV_NAME="COMP5339A2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"

if ! command -v conda >/dev/null 2>&1; then
    echo "conda is required but was not found." >&2
    exit 1
fi

if ! command -v python >/dev/null 2>&1; then
    echo "python not found on PATH. Activate ${ENV_NAME} before running this script." >&2
    exit 1
fi

API_KEY_FILE="${PROJECT_ROOT}/API_key.txt"
if [[ -z "${OE_API_KEY:-}" ]]; then
    if [[ -f "${API_KEY_FILE}" ]]; then
        OE_API_KEY="$(awk -F': ' '/API KEY/ {print $2; exit}' "${API_KEY_FILE}" | tr -d '\r')"
        export OE_API_KEY
        echo "Loaded OE_API_KEY from ${API_KEY_FILE}"
    else
        echo "OE_API_KEY not set and API_key.txt missing; cannot run tests." >&2
        exit 1
    fi
else
    echo "Using OE_API_KEY already present in the environment."
fi

python - <<'PY'
import os, requests, sys
api_key = os.environ.get("OE_API_KEY")
if not api_key:
    sys.exit("OE_API_KEY missing in environment.")
resp = requests.get(
    "https://api.openelectricity.org.au/v4/me",
    headers={"Authorization": f"Bearer {api_key}"},
    timeout=30,
)
if resp.status_code != 200:
    sys.exit(f"API key validation failed ({resp.status_code}): {resp.text}")
print("API key validation succeeded for account:", resp.json().get("data", {}).get("email"))
PY

echo "Running backend build smoke test..."
A2_START_VALUE="${A2_START:-2024-10-01T00:00:00}"
A2_END_VALUE="${A2_END:-2024-10-01T00:15:00}"
BUILD_FLAGS=(--mode build --facility ADP --log-level INFO)
if [[ "${BUILD_DRY_RUN:-0}" -eq 1 ]]; then
    BUILD_FLAGS+=(--dry-run)
fi
A2_START="${A2_START_VALUE}" A2_END="${A2_END_VALUE}" python "${PROJECT_ROOT}/a2_backend.py" "${BUILD_FLAGS[@]}"

STREAM_FLAGS=(--mode stream --log-level INFO)
STREAM_LABEL="stream (publish)"
if [[ "${STREAM_DRY_RUN:-0}" -eq 1 ]]; then
    STREAM_FLAGS+=(--dry-run)
    STREAM_LABEL="stream (dry-run)"
fi
echo "Running backend ${STREAM_LABEL}..."
A2_START="${A2_START_VALUE}" A2_END="${A2_END_VALUE}" \
MQTT_HOST="${MQTT_HOST:-test.mosquitto.org}" \
MQTT_PORT="${MQTT_PORT:-1883}" \
python "${PROJECT_ROOT}/a2_backend.py" "${STREAM_FLAGS[@]}"

echo "All tests completed successfully."
