COMP5339 Assignment 2 – Minimal Backend + Frontend
==================================================

Overview
--------
This repository satisfies the COMP5339 Assignment 2 brief:

1. ``a2_backend.py`` fetches OpenElectricity facility metadata and metrics,
   materialises a single CSV cache (``data/cache/{network}_metrics.csv``), and
   publishes only NEW rows to MQTT in build/stream/loop modes.
2. ``a2_frontend.py`` seeds from the CSV cache, subscribes to MQTT, and renders
   the live dashboard required for Task 4.
3. ``requirements.txt`` lists the minimal shared dependencies.

Environment setup
-----------------
1. Use Python 3.10+ (``python --version`` should report ≥3.10).
2. Install dependencies once (venv or user site are both fine):
   ```bash
   python -m pip install -r requirements.txt
   ```
3. Ensure cache folders exist (the backend also creates them if missing):
   ```bash
   mkdir -p data/cache data/tmp
   ```

Configuration (no environment variables needed)
-----------------------------------------------
All required settings live inside the code so the assignment can be run without
exporting environment variables:

- ``a2_backend.py`` defines ``DEFAULT_API_KEY``, ``DEFAULT_START`` (2025‑10‑01),
  ``DEFAULT_END`` (one week later), cache paths, MQTT host/port, etc.
- ``a2_frontend.py`` defaults to ``test.mosquitto.org:1883`` and
  ``data/cache`` for seeding.

To change any value, either modify the constants near the top of the respective
file or supply CLI overrides (e.g., ``--start``, ``--end``, ``--network``,
``--loop-delay``). No environment variables are read anywhere in the code.

Running the backend
-------------------
```bash
# Build / extend the CSV cache for a specific window (defaults cover 2025-10-01 → 2025-10-08)
python a2_backend.py --mode build --network NEM --window-hours 24

# Publish only the rows that have not been seen before (uses publish_offsets.json)
python a2_backend.py --mode stream

# Continuous execution: stream → backfill → sleep(loop_delay)
python a2_backend.py --mode loop --loop-delay 120
```

Cache location, row counts, and covered timespan
------------------------------------------------
- Metrics CSV: ``data/cache/{network}_metrics.csv`` (e.g. ``data/cache/nem_metrics.csv``)
- Facilities metadata: ``data/cache/facilities.csv``
- Publish watermark: ``data/cache/publish_offsets.json``

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
1. Start ``python a2_backend.py --mode stream`` (or ``--mode loop``).
2. Subscribe to the topics (example uses mosquitto-tools):
```bash
mosquitto_sub -h "localhost" -p "1883" -t 'nem/#' -v
```
   Messages appear in event-time order with ≥0.1 s spacing and include
   ``facility_id``, ``ts_event``, ``power_mw``, and ``co2_t``.

Running the frontend (Task 4)
-----------------------------
```bash
streamlit run a2_frontend.py
```
The dashboard loads the CSV seed from ``data/cache`` (override via CLI) and then
listens to ``nem/+/+/#`` by default. Use the sidebar to filter by fuel or
network region, reload the cache, or toggle auto-refresh.

Troubleshooting
---------------
- "CSV cache not found" – run build mode once before streaming/frontend.
- "No new rows after watermark" – delete ``publish_offsets.json`` if you need
  to replay historic data.
- MQTT connection errors – verify the broker defined inside ``a2_backend.py`` /
  ``a2_frontend.py`` (or override via CLI) and ensure the firewall allows access.

Evidence checklist
------------------
Capture (screenshots/logs) for submission:
1. Backend build logs showing successful API fetch + CSV append + manifest entry.
2. Backend stream logs showing MQTT publishes at ≥0.1 s with advancing watermark.
3. Streamlit dashboard screenshot proving markers update from live MQTT events.
