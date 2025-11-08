COMP5339 Assignment 2 – Minimal Backend + Frontend
==================================================

Overview
--------
This repository satisfies the COMP5339 Assignment 2 brief:

1. `a2_backend.py` fetches OpenElectricity facility metadata and metrics,
   writes a single CSV cache (`data/cache/{network}_metrics.csv`), and publishes
   only NEW rows to MQTT in build/stream/loop modes.
2. `a2_frontend.py` seeds from the CSV cache, subscribes to MQTT, and renders
   the live dashboard required for Task 4.
3. `requirements.txt` lists the minimal shared dependencies.

Environment setup
-----------------
1. Use Python 3.10+ (`python --version` should report ≥3.10).
2. Install dependencies once (venv or user site are both fine):
   ```bash
   python -m pip install -r requirements.txt
   ```
3. Ensure cache folders exist (the backend also creates them if missing):
   ```bash
   mkdir -p data/cache data/tmp
   ```

MQTT broker requirement
-----------------------
Both backend and frontend rely on an MQTT broker. The default host/port/topic
are `localhost:1883` and `nem/events`. Install Mosquitto (or any MQTT v3/v5
broker) and start it before running either script.

Windows examples (Mosquitto installer places binaries in
`C:\Program Files\mosquitto`):

```powershell
# Foreground mode with verbose logs (Ctrl+C to stop)
& "C:\Program Files\mosquitto\mosquitto.exe" -v

# OR run as a Windows service in the background
net start Mosquitto
# later -> net stop Mosquitto
```

If you prefer Docker:
```bash
docker run --name mosquitto -p 1883:1883 eclipse-mosquitto
```

Running the backend
-------------------
```bash
# Build / extend the CSV cache for a specific window
python a2_backend.py --mode build --network NEM --window-hours 24

# Publish only the rows that have not been seen before (uses publish_offsets.json)
python a2_backend.py --mode stream

# Continuous execution: stream → backfill → sleep(loop_delay)
python a2_backend.py --mode loop --loop-delay 120
```

Cache location, row counts, and covered timespan
------------------------------------------------
- Metrics CSV: `data/cache/{network}_metrics.csv` (e.g. `data/cache/nem_metrics.csv`)
- Facilities metadata: `data/cache/facilities.csv`
- Publish watermark: `data/cache/publish_offsets.json`

Check cache basics anytime:
```bash
python - <<'PY'
import pandas as pd
from pathlib import Path
cache = Path('data/cache/nem_metrics.csv')
if not cache.exists():
    raise SystemExit('Cache missing; run build mode first')
df = pd.read_csv(cache, parse_dates=['ts_event'])
print(f"rows={len(df)} facilities={df['facility_id'].nunique()}")
print(f"window={df['ts_event'].min()} -> {df['ts_event'].max()}")
PY
```

Verifying MQTT publishing
-------------------------
1. Start `python a2_backend.py --mode stream` (or `--mode loop`).
2. Subscribe to the topic (example uses mosquitto-tools):
   ```bash
   mosquitto_sub -h "localhost" -p "1883" -t 'nem/events' -v
   ```
   Messages appear in event-time order with ≥0.1 s spacing and include
   `facility_id`, `ts_event`, `power_mw`, and `emissions_t`.

Running the frontend (Task 4)
-----------------------------
```bash
streamlit run a2_frontend.py
```
The dashboard loads the CSV seed from `data/cache` (override via CLI) and then
listens to `nem/events` by default. Use the sidebar to filter by fuel or
network region, edit the MQTT host/port/topic, reload the cache, or toggle
auto-refresh.

Troubleshooting
---------------
- “Cache not found” – run backend build mode once before streaming/frontend.
- “MQTT status: Disconnected” – ensure a broker is running on the configured
  host/port and no firewall is blocking it. On Windows, verify the Mosquitto
  service or console instance is still active (see broker section above).
- “No new rows after watermark” – delete `publish_offsets.json` if you need to
  replay historic data.

Evidence checklist
------------------
Capture (screenshots/logs) for submission:
1. Backend build logs showing successful API fetch + CSV append + manifest entry.
2. Backend stream logs showing MQTT publishes at ≥0.1 s with advancing watermark.
3. Streamlit dashboard screenshot proving markers update from live MQTT events.
