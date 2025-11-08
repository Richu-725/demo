"""COMP5339 Assignment 2 backend (Tasks 1–3 with continuous loop).

This module keeps the proven “one facility + one metric per request” fetch
strategy from ``Comp5339A2_TUT19_Group08_main.ipynb`` and layers on the extra
features mandated by the assignment brief:

* CSV cache that grows as soon as new rows arrive (Task 2).
* MQTT publishing with ≥0.1 s delay between messages (Task 3).
* Continuous loop that alternates between building the cache and streaming it
  with a configurable pause (Task 5).

Typical usage (from the repo root):

    # Build one week of data (required window: October 2025)
    python a2_backend.py --mode build --start 2025-10-05T00:00:00 --end   2025-10-12T00:00:00 --metrics energy,emissions

    # Publish cached rows over MQTT at ≥0.1 s spacing
    python a2_backend.py --mode stream --start 2025-10-05T00:00:00 --end 2025-10-12T00:00:00

    # Continuous pipeline (build → stream → 60s sleep)
    python a2_backend.py --mode loop --start 2025-10-05T00:00:00 --end 2025-10-12T00:00:00 --loop-delay 120

Each public function/docstring spells out the inputs/outputs so the frontend
team can safely import and reuse pieces without triggering side effects.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd
import requests
from paho.mqtt import client as mqtt

# --------------------------------------------------------------------------- #
# Constants & helpers
# --------------------------------------------------------------------------- #

BASE_URL = "https://api.openelectricity.org.au/v4"
FACILITIES_ENDPOINT = "/facilities/"
DATA_ENDPOINT = "/data/facilities/{network}"
CACHE_DIR = Path("data/cache")
METRICS_CSV = CACHE_DIR / "nem_metrics.csv"
FACILITIES_CSV = CACHE_DIR / "facilities.csv"
MANIFEST_CSV = CACHE_DIR / "manifest.csv"
DEFAULT_API_KEY = "oe_3ZRd6d2R91fVxAEBekefybDa"
DEFAULT_METRICS = ("energy", "emissions")
FETCH_DELAY = 0.2  # seconds between API calls (matches notebook)
PUBLISH_DELAY = 0.1  # MQTT requirement

METRIC_COLUMN_MAP = {
    "energy": "energy_mwh",
    "emissions": "emissions_t",
    "price": "price_per_mwh",
    "demand": "demand_mw",
}
METRIC_ALIASES = {
    "power": "energy",
}


def _canonical_metric(metric: str) -> str:
    """Return the API-facing metric name (power -> energy)."""
    return METRIC_ALIASES.get(metric.lower(), metric.lower())


def _parse_iso(value: str) -> datetime:
    """Parse ISO timestamps into timezone-naive datetimes."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def _interval_to_hours(interval: str) -> float:
    """Convert strings like '5m' into fractional hours."""
    suffix = interval[-1]
    amount = int(interval[:-1])
    if suffix == "s":
        return amount / 3600.0
    if suffix == "m":
        return amount / 60.0
    if suffix == "h":
        return float(amount)
    raise ValueError(f"Unsupported interval '{interval}'. Use formats such as 5m or 1h.")


# --------------------------------------------------------------------------- #
# Settings & configuration
# --------------------------------------------------------------------------- #


@dataclass
class Settings:
    """Centralised runtime configuration, overridable via CLI."""

    mode: str
    start: datetime
    end: datetime
    metrics: List[str] = field(default_factory=lambda: list(DEFAULT_METRICS))
    interval: str = "5m"
    network: str = "NEM"
    api_key: str = DEFAULT_API_KEY
    cache_dir: Path = CACHE_DIR
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_topic: str = "nem/events"
    loop_delay: float = 60.0


def parse_args(argv: Optional[Iterable[str]] = None) -> Settings:
    """Parse CLI arguments into a Settings instance."""
    parser = argparse.ArgumentParser(description="COMP5339 Assignment 2 backend")
    parser.add_argument("--mode", choices=["build", "stream", "loop"], required=True)
    parser.add_argument("--start", type=_parse_iso, required=True)
    parser.add_argument("--end", type=_parse_iso, required=True)
    parser.add_argument("--metrics", help="Comma-separated metrics (default: energy,emissions)")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--network", default="NEM")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY)
    parser.add_argument("--cache-dir", default=str(CACHE_DIR))
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-topic", default="nem/events")
    parser.add_argument("--loop-delay", type=float, default=60.0)
    args = parser.parse_args(list(argv) if argv is not None else None)

    metrics = (
        [part.strip() for part in args.metrics.split(",") if part.strip()]
        if args.metrics
        else list(DEFAULT_METRICS)
    )

    return Settings(
        mode=args.mode,
        start=args.start,
        end=args.end,
        metrics=metrics,
        interval=args.interval,
        network=args.network,
        api_key=args.api_key,
        cache_dir=Path(args.cache_dir),
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_topic=args.mqtt_topic,
        loop_delay=max(args.loop_delay, 60.0),
    )


# --------------------------------------------------------------------------- #
# Facility metadata
# --------------------------------------------------------------------------- #


def discover_facilities(settings: Settings) -> pd.DataFrame:
    """Fetch facility metadata, write facilities.csv, and return a DataFrame."""
    headers = {"Authorization": f"Bearer {settings.api_key}"}
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    fac_path = settings.cache_dir / FACILITIES_CSV.name
    if fac_path.exists():
        try:
            cached = pd.read_csv(fac_path)
            if not cached.empty:
                logging.info("Loaded %s facilities from %s", len(cached), fac_path)
                return cached
        except Exception:  # pragma: no cover - fallback to re-fetch
            logging.warning("Failed to read %s; refetching metadata.", fac_path)

    resp = requests.get(f"{BASE_URL}{FACILITIES_ENDPOINT}", headers=headers, timeout=60)
    resp.raise_for_status()
    rows: List[Dict[str, object]] = []
    for facility in resp.json().get("data", []):
        if facility.get("network_id") != settings.network:
            continue
        code = facility.get("code")
        loc = facility.get("location") or {}
        if not code or loc.get("lat") is None or loc.get("lng") is None:
            continue
        units = facility.get("units") or []
        fuels = [unit.get("fueltech_id") for unit in units if unit.get("fueltech_id")]
        rows.append(
            {
                "facility_id": code,
                "name": facility.get("name"),
                "fuel": fuels[0] if fuels else "unknown",
                "state": (facility.get("network_region") or "")[:2],
                "lat": loc.get("lat"),
                "lon": loc.get("lng"),
            }
        )
    df = pd.DataFrame(rows).sort_values("facility_id").reset_index(drop=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    fac_path = settings.cache_dir / FACILITIES_CSV.name
    df.to_csv(fac_path, index=False)
    logging.info("Discovered %s facilities; cached metadata at %s", len(df), fac_path)
    return df


# --------------------------------------------------------------------------- #
# Metrics fetching & transformation
# --------------------------------------------------------------------------- #


def fetch_metric_series(settings: Settings, facility_id: str, metric: str) -> List[Dict[str, object]]:
    """Fetch a single metric series for a facility, respecting the notebook timing."""
    headers = {"Authorization": f"Bearer {settings.api_key}"}
    params = [
        ("metrics", metric),
        ("facility_code", facility_id),
        ("interval", settings.interval),
        ("date_start", settings.start.strftime("%Y-%m-%dT%H:%M:%S")),
        ("date_end", settings.end.strftime("%Y-%m-%dT%H:%M:%S")),
    ]
    resp = requests.get(
        f"{BASE_URL}{DATA_ENDPOINT.format(network=settings.network)}",
        headers=headers,
        params=params,
        timeout=60,
    )
    if resp.status_code == 416:
        logging.debug("facility=%s metric=%s status=no_data", facility_id, metric)
        return []
    resp.raise_for_status()
    rows: List[Dict[str, object]] = []
    for block in resp.json().get("data", []):
        for result in block.get("results", []):
            unit_code = (result.get("columns") or {}).get("unit_code")
            for ts_raw, value in result.get("data", []):
                ts_event = datetime.fromisoformat(ts_raw)
                if ts_event.tzinfo is not None:
                    ts_event = ts_event.replace(tzinfo=None)
                rows.append(
                    {
                        "facility_id": facility_id,
                        "metric": metric,
                        "ts_event": ts_event,
                        "value": value,
                        "unit_code": unit_code,
                    }
                )
    return rows


def _pivot_and_enrich(
    facility_frames: List[pd.DataFrame],
    facilities_meta: pd.DataFrame,
    interval_hours: float,
) -> pd.DataFrame:
    """Combine metric frames, pivot to wide format, and join metadata."""
    metrics_df = pd.concat(facility_frames, ignore_index=True)
    pivoted = (
        metrics_df.pivot_table(
            index=["facility_id", "ts_event"],
            columns="metric",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .rename(columns=METRIC_COLUMN_MAP)
    )
    if "energy_mwh" in pivoted and interval_hours > 0:
        pivoted["power_mw"] = pivoted["energy_mwh"] / interval_hours
    enriched = pivoted.merge(facilities_meta, on="facility_id", how="left")
    enriched["ts_event"] = enriched["ts_event"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    enriched["ts_ingest"] = datetime.utcnow().isoformat()
    columns = [
        "facility_id",
        "ts_event",
        "ts_ingest",
        "power_mw",
        "energy_mwh",
        "emissions_t",
        "demand_mw",
        "price_per_mwh",
        "name",
        "fuel",
        "state",
        "lat",
        "lon",
    ]
    for column in columns:
        if column not in enriched:
            enriched[column] = pd.NA
    return enriched[columns]


def append_to_cache(df: pd.DataFrame, cache_path: Path) -> None:
    """Append facility rows to the metrics CSV in event-time order."""
    header = not cache_path.exists()
    df.sort_values("ts_event").to_csv(cache_path, mode="a", header=header, index=False)
    logging.info("Appended %s rows to %s", len(df), cache_path)


def update_manifest(cache_path: Path, manifest_path: Path, window_label: str) -> None:
    """Record cache metadata (bytes + timestamp) for reproducibility."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "window": window_label,
        "cache_path": str(cache_path),
        "bytes": cache_path.stat().st_size if cache_path.exists() else 0,
        "retrieved_at": datetime.utcnow().isoformat(),
    }
    if manifest_path.exists():
        df = pd.read_csv(manifest_path)
        if "window" not in df.columns:
            df = pd.DataFrame(columns=entry.keys())
    else:
        df = pd.DataFrame(columns=entry.keys())
    df = df[df["window"] != window_label]
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    df.to_csv(manifest_path, index=False)


# --------------------------------------------------------------------------- #
# MQTT publishing
# --------------------------------------------------------------------------- #


def publish_events(settings: Settings, cache_path: Path) -> None:
    """Publish cached rows over MQTT, ensuring ≥0.1 s between messages."""
    if not cache_path.exists() or cache_path.stat().st_size == 0:
        logging.warning("Cache %s missing/empty; nothing to publish.", cache_path)
        return
    df = pd.read_csv(cache_path)
    if df.empty:
        logging.warning("Cache %s contains no rows; nothing to publish.", cache_path)
        return

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
    client.loop_start()
    try:
        for _, row in df.sort_values("ts_event").iterrows():
            payload = json.dumps(
                {
                    "facility_id": row["facility_id"],
                    "facility_name": row.get("name"),
                    "fuel": row.get("fuel"),
                    "state": row.get("state"),
                    "lat": row.get("lat"),
                    "lon": row.get("lon"),
                    "ts_event": row.get("ts_event"),
                    "ts_ingest": row.get("ts_ingest"),
                    "power_mw": row.get("power_mw"),
                    "energy_mwh": row.get("energy_mwh"),
                    "emissions_t": row.get("emissions_t"),
                    "demand_mw": row.get("demand_mw"),
                    "price_per_mwh": row.get("price_per_mwh"),
                }
            )
            client.publish(settings.mqtt_topic, payload, qos=0)
            logging.info("Published event ts=%s", row.get("ts_event"))
            time.sleep(max(PUBLISH_DELAY, 0.1))
    finally:
        client.loop_stop()
        client.disconnect()


# --------------------------------------------------------------------------- #
# Orchestration modes
# --------------------------------------------------------------------------- #


def _load_checkpoint(cache_path: Path, manifest_path: Path, window_label: str) -> Set[str]:
    """Return facility_ids already cached for the given window (if any)."""
    if not cache_path.exists() or not manifest_path.exists():
        if cache_path.exists():
            cache_path.unlink()
        return set()
    manifest = pd.read_csv(manifest_path)
    if "window" not in manifest.columns:
        cache_path.unlink()
        return set()
    row = manifest[manifest["window"] == window_label]
    if row.empty:
        cache_path.unlink()
        return set()
    try:
        df = pd.read_csv(cache_path, usecols=["facility_id"])
    except Exception:
        cache_path.unlink()
        return set()
    processed = set(df["facility_id"].dropna().unique())
    logging.info("Resuming window %s with %s facilities already cached.", window_label, len(processed))
    return processed


def build_cache(settings: Settings) -> Path:
    """Fetch metrics for the configured window and append rows to the CSV cache."""
    facilities = discover_facilities(settings)
    cache_path = settings.cache_dir / METRICS_CSV.name
    manifest_path = settings.cache_dir / MANIFEST_CSV.name
    interval_hours = _interval_to_hours(settings.interval)
    canonical_metrics = [_canonical_metric(metric) for metric in settings.metrics]
    window_label = f"{settings.start:%Y%m%d%H%M}-{settings.end:%Y%m%d%H%M}"
    processed_ids = _load_checkpoint(cache_path, manifest_path, window_label)

    for idx, facility in facilities.iterrows():
        fid = facility["facility_id"]
        if fid in processed_ids:
            logging.info("Skipping facility %s (already cached for this window).", fid)
            continue
        logging.info("Processing facility %s/%s (%s)", idx + 1, len(facilities), fid)
        metric_frames: List[pd.DataFrame] = []
        for metric in canonical_metrics:
            rows = fetch_metric_series(settings, fid, metric)
            if rows:
                metric_frames.append(pd.DataFrame.from_records(rows))
                logging.info("facility=%s metric=%s rows=%s", fid, metric, len(rows))
            time.sleep(FETCH_DELAY)
        if not metric_frames:
            continue
        enriched = _pivot_and_enrich(metric_frames, facilities, interval_hours)
        append_to_cache(enriched, cache_path)
        processed_ids.add(fid)

    if not cache_path.exists():
        cache_path.write_text("")
        logging.warning("No metric data available for the requested window.")

    update_manifest(
        cache_path,
        manifest_path,
        window_label=window_label,
    )
    return cache_path


def stream_cache(settings: Settings) -> None:
    """Publish the most recent cache over MQTT."""
    publish_events(settings, settings.cache_dir / METRICS_CSV.name)


def loop_pipeline(settings: Settings) -> None:
    """Run build → stream → sleep indefinitely (Task 5)."""
    logging.info("Starting loop with %.1fs delay between iterations.", settings.loop_delay)
    while True:
        try:
            build_cache(settings)
            stream_cache(settings)
        except Exception as exc:  # pragma: no cover - defensive logging
            logging.exception("Loop iteration failed: %s", exc)
        time.sleep(settings.loop_delay)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main(argv: Optional[Iterable[str]] = None) -> None:
    """CLI entry point that dispatches to the requested mode."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    settings = parse_args(argv)
    if settings.mode == "build":
        build_cache(settings)
    elif settings.mode == "stream":
        stream_cache(settings)
    elif settings.mode == "loop":
        loop_pipeline(settings)
    else:  # pragma: no cover
        raise ValueError(f"Unsupported mode {settings.mode}")


if __name__ == "__main__":
    main()
