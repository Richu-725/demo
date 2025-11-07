= COMP5339 Assignment 2 Backend Demo =

This demo contains the data engineering backend for COMP5339 Assignment 2. Follow the steps below to set up the environment, run smoke tests, and execute the pipeline on Windows (PowerShell) or Linux/WSL.

Prerequisites:
- Anaconda/Miniconda installed (conda command available).
- OpenElectricity API key stored in `API_key.txt` (already included here).
- Git Bash or WSL recommended for Windows users to run the provided shell scripts. Use `.\gbash.cmd` from this folder if you want to launch Git Bash without shadowing the WSL `bash` on `PATH`.

Directory overview:
- `a2_backend.py` – backend pipeline CLI.
- `a2_frontend.py` – Streamlit dashboard (Task 4).
- `setup_comp5339a2.sh` – bootstraps the conda env and exports helpful vars.
- `test_comp5339a2.sh` – smoke tests (build + live stream publish).
- `test_comp5339a2_frontend.sh` – headless frontend smoke test.
- `requirements.txt` – Python dependencies.
- `envs.txt` – copy/paste env var snippets if you prefer to set them manually.
- `data/` – cache and tmp folders created automatically.

----------------------------------------------------------------------
Quick start (Linux / WSL / Git Bash)
----------------------------------------------------------------------
1. cd into the project folder (where this README lives).
2. Run: `source ./setup_comp5339a2.sh`
   - Creates/updates the `COMP5339A2` conda env.
   - Installs requirements.
   - Exports `OE_API_KEY`, `A2_CACHE_DIR`, and `A2_TMP_DIR` pointing to the current folder.
3. Optional: verify with `conda run -n COMP5339A2 python -m pip list` or by running the smoke tests (next section).

To run the smoke tests:
`./test_comp5339a2.sh`
- Runs the build step (real API calls) and publishes to the public MQTT broker at `test.mosquitto.org:1883` by default.
- To switch back to dry-run streaming, set `STREAM_DRY_RUN=1` before running the script or override `MQTT_HOST`/`MQTT_PORT` to target your own broker.
- Frontend smoke test: `FRONTEND_PYTHON=python ./test_comp5339a2_frontend.sh` (WSL) or `./test_comp5339a2_frontend.sh` (macOS/Windows). Use `FRONTEND_TEST_DURATION`/`FRONTEND_TEST_PORT` to adjust runtime/port.

To run the backend manually:
```
conda run -n COMP5339A2 env \
  OE_API_KEY="$OE_API_KEY" \
  A2_START="2024-10-01T00:00:00" \
  A2_END="2024-10-08T00:00:00" \
  python a2_backend.py --mode build --log-level INFO
```
Adjust start/end times and options as needed. To stream the cached data: `python a2_backend.py --mode stream`.

Loop mode (`--mode loop`) refreshes the cache every 60 seconds and streams after each build until interrupted (Ctrl+C).

----------------------------------------------------------------------
Quick start (Windows PowerShell)
----------------------------------------------------------------------
- Open an **Anaconda PowerShell Prompt**, `Set-Location` to the repo, then `conda activate COMP5339A2`.
- Run:
  ```powershell
  .\setup_comp5339a2.sh
  ```
  This installs/upgrades dependencies (pandas 2.1, requests 2.32, paho-mqtt 2.1, pyarrow 21.0, pandera 0.26, streamlit 1.51, pydeck 0.9, protobuf 3.20, watchdog 6.0, tenacity 9.1, toml 0.10, altair 5.5, cachetools 6.2, gitpython 3.1), exports `OE_API_KEY`, `A2_*` paths, and prints the cache/temp directories.
- Smoke tests:
  ```powershell
  .\test_comp5339a2.sh            # Backend build + live publish to test.mosquitto.org
  .\test_comp5339a2_frontend.sh   # Headless Streamlit check
  ```
- Manual backend run:
  ```powershell
  $env:OE_API_KEY = 'oe_3ZgRMZCTomF3BpPcK3TzLCzs'
  $env:A2_START   = '2024-10-01T00:00:00'
  $env:A2_END     = '2024-10-01T00:15:00'
  $env:MQTT_HOST  = 'test.mosquitto.org'
  python .\a2_backend.py --mode build --facility ADP --log-level INFO

  $env:MQTT_HOST  = 'test.mosquitto.org'
  python .\a2_backend.py --mode stream --log-level INFO
  ```
- Launch the dashboard:
  ```powershell
  python -m streamlit run .\a2_frontend.py `
      --server.headless true `
      --server.port 8631 `
      --browser.gatherUsageStats false
  ```

----------------------------------------------------------------------
Frontend (Task 4)
----------------------------------------------------------------------
1. Ensure a recent cache exists (`python a2_backend.py --mode build`) and that the backend is publishing (`python a2_backend.py --mode stream`).
2. Launch the dashboard with `streamlit run a2_frontend.py`.
   - Defaults connect to the public broker `test.mosquitto.org:1883`. Override via `MQTT_HOST`/`MQTT_PORT`.
   - The app auto-refreshes every few seconds; toggle this in the sidebar or adjust `FRONTEND_REFRESH_SEC`.
3. Use the sidebar filters to focus on specific fuel types or network regions and inspect the live markers.
4. If the Assignment 1 CSVs (e.g., `power-stations-and-projects-*.csv`) are present in the project root, the dashboard automatically loads them to seed additional facility markers (capacity, status, location). No extra setup required.
5. Reload the cache from the sidebar if you rebuild the backend with a different time window.

----------------------------------------------------------------------
Notes and tips
----------------------------------------------------------------------
- Always include `OE_API_KEY` in your environment before running scripts. You can read it from `API_key.txt` or supply your own.
- The backend expects naive (timezone-free) ISO timestamps in network-local time (for NEM, use AEDT/AEST with no timezone offset).
- The scripts enforce a request budget, rate-limiting, and manifest tracking to keep API usage within daily limits.
- If you move the project folder, rerun `setup_comp5339a2.sh` or reapply the env var snippets so cache/temp paths update.
- The `data/` directory is ignored during dry runs; the first real build (`--dry-run` removed) will write a parquet file and update `data/cache/manifest.csv`.
- To publish events to an MQTT broker, remove `--dry-run` from stream mode and ensure `MQTT_HOST`/`MQTT_PORT` are set (defaults to `localhost:1883`).
- Facility metadata is cached at `data/cache/facilities_catalog.parquet` (24h TTL by default). Override `A2_FACILITY_CACHE_TTL` or remove the file to force a refresh.

----------------------------------------------------------------------
Usage tips
----------------------------------------------------------------------
- Cache responses whenever possible so subsequent runs reuse the parquet artefacts instead of re-calling the API.
- Use the existing manifest and cache files to batch downstream analysis rather than requesting the same window repeatedly.
- Pass `--facility-cache-only` (or set `A2_FACILITY_CACHE_ONLY=1`) when you want to reuse the facility catalogue without hitting the discovery endpoint.
- Pass `--cache-only` (or set `A2_CACHE_ONLY=1`) to recycle an existing metrics parquet and skip all API requests.
- Monitor log output for `API usage summary` lines to understand how quickly you are consuming the request budget.
- Adjust `A2_BUDGET_ALERT_THRESHOLD` to receive early warnings in the logs as you approach quota limits.
- Build larger time windows less frequently and stream from cache to minimise total API calls.

Troubleshooting:
- “Missing OE_API_KEY” – ensure the env var is set in the same shell running the command.
- “Invalid header value … \r” – run the scripts in WSL/Git Bash or convert `API_key.txt` to Unix line endings (`dos2unix API_key.txt`); the setup script already strips carriage returns.
- “No cache files found” – run a build mode without `--dry-run` to create the first parquet cache before streaming.
- `conda` not found in Git Bash – run `conda init bash` from an Anaconda Prompt, then restart the terminal, or run scripts via `wsl bash`.

----------------------------------------------------------------------
Next steps
----------------------------------------------------------------------
- Adjust `envs.txt` for custom windows (dates, facility subsets) if needed.
- Consider adding automated tests or CI pipelines using the provided scripts as templates.

For further details consult `a2_backend.py` docstrings and inline comments.
