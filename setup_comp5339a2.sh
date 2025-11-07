#!/usr/bin/env bash
# Bootstrap the COMP5339 Assignment 2 backend environment.
# Usage: source ./setup_comp5339a2.sh  (so exported vars persist in your shell)

set -euo pipefail

ENV_NAME="COMP5339A2"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements.txt"
API_KEY_FILE="${PROJECT_ROOT}/API_key.txt"

if ! command -v conda >/dev/null 2>&1; then
    echo "conda is required but was not found. Install Anaconda/Miniconda before running this script." >&2
    return 1 2>/dev/null || exit 1
fi

if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then
    echo "requirements.txt not found at ${REQUIREMENTS_FILE}" >&2
    return 1 2>/dev/null || exit 1
fi

if ! conda env list | awk '{print $1}' | grep -qx "${ENV_NAME}"; then
    echo "Creating conda environment ${ENV_NAME} (python=${PYTHON_VERSION})..."
    conda create -y -n "${ENV_NAME}" "python=${PYTHON_VERSION}"
else
    echo "Conda environment ${ENV_NAME} already exists."
fi

echo "Upgrading pip and installing requirements into ${ENV_NAME}..."
conda run -n "${ENV_NAME}" python -m pip install --upgrade pip
conda run -n "${ENV_NAME}" python -m pip install -r "${REQUIREMENTS_FILE}"

if [[ -f "${API_KEY_FILE}" ]]; then
    OE_API_KEY="$(awk -F': ' '/API KEY/ {print $2; exit}' "${API_KEY_FILE}" | tr -d '\r')"
    if [[ -n "${OE_API_KEY:-}" ]]; then
        export OE_API_KEY
        echo "Exported OE_API_KEY from ${API_KEY_FILE}"
    else
        echo "API_key.txt present but OE_API_KEY could not be parsed; skipping export." >&2
    fi
else
    echo "API_key.txt not found; set OE_API_KEY manually before running tests." >&2
fi

# Sensible defaults for backend execution.
DEFAULT_CACHE_DIR="${PROJECT_ROOT}/data/cache"
DEFAULT_TMP_DIR="${PROJECT_ROOT}/data/tmp"

export A2_NETWORK="${A2_NETWORK:-NEM}"
export A2_CACHE_DIR="${A2_CACHE_DIR:-${DEFAULT_CACHE_DIR}}"
export A2_TMP_DIR="${A2_TMP_DIR:-${DEFAULT_TMP_DIR}}"

mkdir -p "${A2_CACHE_DIR}" "${A2_TMP_DIR}"

# Friendly message with Windows path if running under WSL/MSYS.
DISPLAY_CACHE="${A2_CACHE_DIR}"
DISPLAY_TMP="${A2_TMP_DIR}"
if command -v wslpath >/dev/null 2>&1; then
    DISPLAY_CACHE="$(wslpath -w "${A2_CACHE_DIR}")"
    DISPLAY_TMP="$(wslpath -w "${A2_TMP_DIR}")"
elif command -v cygpath >/dev/null 2>&1; then
    DISPLAY_CACHE="$(cygpath -w "${A2_CACHE_DIR}")"
    DISPLAY_TMP="$(cygpath -w "${A2_TMP_DIR}")"
fi

cat <<EOF
Environment ready.
- Conda env: ${ENV_NAME}
- Python    : ${PYTHON_VERSION}
- Requirements installed from ${REQUIREMENTS_FILE}
- Cache dir : ${DISPLAY_CACHE}
- Temp dir  : ${DISPLAY_TMP}

Remember to keep this script sourced so OE_API_KEY and A2_* exports remain in your shell.
To activate the environment manually: conda activate ${ENV_NAME}
Backend smoke test: ./test_comp5339a2.sh
Frontend smoke test: ./test_comp5339a2_frontend.sh (set FRONTEND_PYTHON=python when running under WSL)
EOF
