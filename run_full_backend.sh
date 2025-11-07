#!/usr/bin/env bash
# Orchestrate the backend build + stream for COMP5339 A2 with a broad facility set.
# Usage:
#   ./run_full_backend.sh              # real build/stream using all detected facility IDs
#   BUILD_DRY_RUN=1 ./run_full_backend.sh   # skip cache write/publish (passes --dry-run)
#   MAX_FACILITIES=120 ./run_full_backend.sh  # limit facilities to first 120 from the CSVs

set -euo pipefail

ENV_NAME="COMP5339A2"
PYTHON_BIN=(conda run -n "${ENV_NAME}" python)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
SETUP_SCRIPT="${PROJECT_ROOT}/setup_comp5339a2.sh"
FACILITY_CSV="${PROJECT_ROOT}/DevNotes/facility_ids_from_assignment_csvs.csv"

if [[ ! -f "${SETUP_SCRIPT}" ]]; then
    echo "Setup script not found at ${SETUP_SCRIPT}" >&2
    exit 1
fi

# Ensure the environment exists and the shared exports are in place.
if [[ "${SKIP_SETUP:-0}" -ne 1 ]]; then
    source "${SETUP_SCRIPT}"
fi

export OE_API_KEY="${OE_API_KEY:-$(awk -F': ' '/API KEY/ {print $2; exit}' "${PROJECT_ROOT}/API_key.txt" 2>/dev/null | tr -d '\r')}"
if [[ -z "${OE_API_KEY:-}" ]]; then
    echo "OE_API_KEY not set and API_key.txt missing; cannot proceed." >&2
    exit 1
fi

export A2_START="${A2_START:-2024-10-01T00:00:00}"
export A2_END="${A2_END:-2024-10-01T06:00:00}"
export MQTT_HOST="${MQTT_HOST:-test.mosquitto.org}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export STREAM_DRY_RUN="${STREAM_DRY_RUN:-0}"
export BUILD_DRY_RUN="${BUILD_DRY_RUN:-0}"

MAX_FACILITIES="${MAX_FACILITIES:-0}"  # 0 means "no explicit limit"
# Leave some headroom for discovery and manifest calls.
REQUEST_BUDGET="${A2_REQUEST_BUDGET:-450}"
if [[ "${REQUEST_BUDGET}" -le 20 ]]; then
    REQUEST_BUDGET=20
fi
EFFECTIVE_BUDGET=$((REQUEST_BUDGET - 10))
if [[ "${EFFECTIVE_BUDGET}" -lt 1 ]]; then
    EFFECTIVE_BUDGET=REQUEST_BUDGET
fi

if [[ -z "${A2_FACILITY_CACHE_PATH:-}" ]]; then
    export A2_FACILITY_CACHE_PATH="${PROJECT_ROOT}/data/cache/facilities_catalog.parquet"
fi
export A2_FACILITY_CACHE_TTL="${A2_FACILITY_CACHE_TTL:-86400}"

# Collect facility IDs from the generated CSV plus the canonical ADP ID.
FACILITY_IDS=()
FACILITY_IDS+=("ADP")

# Derive candidate facility IDs from the CSV.
if [[ -f "${FACILITY_CSV}" ]]; then
    mapfile -t CSV_IDS < <(
        tail -n +2 "${FACILITY_CSV}" | awk -F',' '{gsub(/"\r?$/,"",$2); gsub(/^"/,"",$2); print $2}' | sed '/^$/d'
    )
    if [[ "${MAX_FACILITIES}" -gt 0 ]]; then
        CSV_IDS=("${CSV_IDS[@]:0:${MAX_FACILITIES}}")
    fi
    FACILITY_IDS+=("${CSV_IDS[@]}")
else
    echo "Warning: ${FACILITY_CSV} not found; running with ADP only." >&2
fi

# Fetch the authoritative facility list (with caching) to ensure the requested codes exist.
echo "Loading facilities catalog (cache: ${A2_FACILITY_CACHE_PATH})..."
if ! FACILITY_CATALOG_OUTPUT="$("${PYTHON_BIN[@]}" - <<'PY'
import os
from pathlib import Path
from a2_backend import OpenElectricityClient, discover_facilities

api_key = os.environ.get("OE_API_KEY")
if not api_key:
    raise SystemExit("OE_API_KEY missing; cannot discover facilities.")

network = os.environ.get("A2_NETWORK", "NEM")
request_budget = int(os.environ.get("A2_REQUEST_BUDGET", "450"))
if request_budget < 10:
    request_budget = 10

cache_path_raw = os.environ.get("A2_FACILITY_CACHE_PATH")
cache_path = Path(cache_path_raw) if cache_path_raw else None
cache_ttl_env = os.environ.get("A2_FACILITY_CACHE_TTL")
cache_ttl = int(cache_ttl_env) if cache_ttl_env not in (None, "") else None

with OpenElectricityClient(api_key, request_budget=request_budget) as client:
    facilities = discover_facilities(
        client,
        network,
        cache_path=cache_path,
        cache_ttl=cache_ttl,
    )

for value in facilities["facility_id"]:
    print(value)
PY
)"; then
    echo "Failed to resolve facilities catalog." >&2
    exit 1
fi
mapfile -t VALID_IDS <<<"${FACILITY_CATALOG_OUTPUT}"
if [[ ${#VALID_IDS[@]} -eq 0 ]]; then
    echo "Facility discovery returned no results; aborting." >&2
    exit 1
fi
declare -A valid_lookup=()
for fid in "${VALID_IDS[@]}"; do
    [[ -n "${fid}" ]] || continue
    valid_lookup["$fid"]=1
done

# Remove duplicate IDs while preserving order.
declare -A seen
FILTERED_IDS=()
for fid in "${FACILITY_IDS[@]}"; do
    if [[ -n "${fid}" && -z "${seen[$fid]:-}" && -n "${valid_lookup[$fid]:-}" ]]; then
        seen["$fid"]=1
        FILTERED_IDS+=("$fid")
    fi
done

TARGET_IDS=("${FILTERED_IDS[@]}")
desired_count="${MAX_FACILITIES}"

append_from_catalog() {
    local current_count="${#TARGET_IDS[@]}"
    for api_id in "${VALID_IDS[@]}"; do
        if [[ -n "${api_id}" && -z "${seen[$api_id]:-}" ]]; then
            TARGET_IDS+=("$api_id")
            seen["$api_id"]=1
            ((current_count++))
            if [[ "${desired_count}" -gt 0 && "${current_count}" -ge "${desired_count}" ]]; then
                break
            fi
        fi
    done
}

if [[ ${#TARGET_IDS[@]} -eq 0 ]]; then
    echo "No CSV facilities matched the API catalog; defaulting to the full catalog."
    append_from_catalog
else
    if [[ "${MAX_FACILITIES}" -gt 0 && ${#TARGET_IDS[@]} < "${MAX_FACILITIES}" ]]; then
        append_from_catalog
    elif [[ "${MAX_FACILITIES}" -eq 0 ]]; then
        append_from_catalog
    fi
fi

if [[ "${MAX_FACILITIES}" -gt 0 && ${#TARGET_IDS[@]} -gt "${MAX_FACILITIES}" ]]; then
    TARGET_IDS=("${TARGET_IDS[@]:0:${MAX_FACILITIES}}")
fi

if [[ ${#TARGET_IDS[@]} -gt "${EFFECTIVE_BUDGET}" ]]; then
    echo "Clipping facility list from ${#TARGET_IDS[@]} to ${EFFECTIVE_BUDGET} to stay within request budget (${REQUEST_BUDGET})."
    TARGET_IDS=("${TARGET_IDS[@]:0:${EFFECTIVE_BUDGET}}")
fi

if [[ ${#TARGET_IDS[@]} -eq 0 ]]; then
    echo "No facility IDs resolved after applying limits; aborting." >&2
    exit 1
fi

FACILITY_ARGS=()
for fid in "${TARGET_IDS[@]}"; do
    FACILITY_ARGS+=(--facility "$fid")
done

echo "Resolved ${#TARGET_IDS[@]} facility IDs."

BUILD_ARGS=(--mode build --log-level INFO "${FACILITY_ARGS[@]}")
if [[ "${BUILD_DRY_RUN}" -eq 1 ]]; then
    BUILD_ARGS+=(--dry-run)
fi

echo "Running backend build..."
"${PYTHON_BIN[@]}" "${PROJECT_ROOT}/a2_backend.py" "${BUILD_ARGS[@]}"

STREAM_ARGS=(--mode stream --log-level INFO)
if [[ "${STREAM_DRY_RUN}" -eq 1 ]]; then
    STREAM_ARGS+=(--dry-run)
fi

echo "Running backend stream..."
"${PYTHON_BIN[@]}" "${PROJECT_ROOT}/a2_backend.py" "${STREAM_ARGS[@]}"

echo "Backend orchestration complete."
