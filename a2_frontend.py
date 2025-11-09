"""Streamlit dashboard for COMP5339 Assignment 2 (Task 4).

This rewrite keeps the original functionality (seed from CSV, listen to MQTT,
render live map/metrics) but adds:

* Shared defaults with the backend (localhost broker + ``nem/events`` topic).
* A sidebar configuration panel (cache path, broker, topic, refresh interval).
* Automatic subscriber restarts whenever settings change.
* Stricter payload validation with clear logging instead of silent skips.
* Defensive handling of missing cache files and MQTT failures.

Run with::

    streamlit run a2_frontend.py --server.headless true --server.port 8501

Optional overrides can be supplied after ``--`` when launching Streamlit, e.g.::

    streamlit run a2_frontend.py -- --mqtt-host test.mosquitto.org --mqtt-topic 'nem/+/+/#'

Broker requirement:

    Ensure Mosquitto (or any MQTT v3/v5 broker) is running on the same host as
    the frontend/backend before starting Streamlit, for example::

        & "C:\\Program Files\\mosquitto\\mosquitto.exe" -v

    or start the Windows service with ``net start Mosquitto``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pydeck as pdk
import streamlit as st
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.frontend")

MARKER_MIN_RADIUS_M = 800
MARKER_MAX_RADIUS_M = 120_000
MARKER_SCALE_FACTOR = 4_000  # multiplies sqrt(power) so high-output sites stay bounded


@dataclass(eq=True)
class FrontendSettings:
    """Runtime configuration used by the dashboard."""

    cache_path: Path = Path("data/cache/nem_metrics.csv")
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_topic: str = "nem/events"
    refresh_interval_s: float = 5.0


DEFAULT_SETTINGS = FrontendSettings()


def parse_runtime_settings(argv: Optional[Iterable[str]] = None) -> FrontendSettings:
    """Allow optional CLI overrides (``streamlit run script.py -- --arg value``)."""

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cache-path", default=str(DEFAULT_SETTINGS.cache_path))
    parser.add_argument("--mqtt-host", default=DEFAULT_SETTINGS.mqtt_host)
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_SETTINGS.mqtt_port)
    parser.add_argument("--mqtt-topic", default=DEFAULT_SETTINGS.mqtt_topic)
    parser.add_argument("--refresh-interval", type=float, default=DEFAULT_SETTINGS.refresh_interval_s)
    args, _ = parser.parse_known_args(list(argv) if argv is not None else None)
    return FrontendSettings(
        cache_path=Path(args.cache_path).expanduser(),
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_topic=args.mqtt_topic,
        refresh_interval_s=max(1.0, float(args.refresh_interval)),
    )


def load_seed_data(path: Path) -> pd.DataFrame:
    """Load and normalise the backend CSV cache."""

    if not path.exists():
        raise FileNotFoundError(f"Cache {path} not found. Run backend build mode first.")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Cache {path} is empty.")

    numeric = ["power_mw", "energy_mwh", "emissions_t", "demand_mw", "price_per_mwh", "lat", "lon"]
    for column in numeric:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ts_event"] = pd.to_datetime(df["ts_event"], errors="coerce")
    if "ts_ingest" in df.columns:
        df["ts_ingest"] = pd.to_datetime(df["ts_ingest"], errors="coerce")
    else:
        df["ts_ingest"] = pd.NaT
    return df.sort_values("ts_event").reset_index(drop=True)


def _maybe_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(num):
        return None
    return num


def _maybe_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure MQTT payloads contain mandatory fields with valid types."""

    facility_id = str(payload.get("facility_id", "")).strip()
    if not facility_id:
        raise ValueError("facility_id missing/blank")
    ts_event = _maybe_timestamp(payload.get("ts_event"))
    if ts_event is None:
        raise ValueError("ts_event missing/unparseable")
    normalized = dict(payload)
    normalized["facility_id"] = facility_id
    normalized["ts_event"] = ts_event
    ts_ingest = _maybe_timestamp(payload.get("ts_ingest"))
    if ts_ingest is not None:
        normalized["ts_ingest"] = ts_ingest
    return normalized


def apply_event(snapshot: pd.DataFrame, payload: Dict[str, Any]) -> pd.DataFrame:
    """Merge one event into the current snapshot, preserving facility metadata."""

    facility_id = payload["facility_id"]
    ts_event = payload["ts_event"]
    if not isinstance(ts_event, pd.Timestamp):
        ts_event = _maybe_timestamp(ts_event)
    if ts_event is None:
        return snapshot

    prior = snapshot[snapshot["facility_id"] == facility_id]
    last_known = prior.iloc[-1].to_dict() if not prior.empty else {}

    def with_fallback(value: Optional[float], field: str) -> Optional[float]:
        return value if value is not None else last_known.get(field)

    row = {
        "facility_id": facility_id,
        "ts_event": ts_event,
        "ts_ingest": _maybe_timestamp(payload.get("ts_ingest")) or last_known.get("ts_ingest") or pd.Timestamp.utcnow(),
        "power_mw": with_fallback(_maybe_float(payload.get("power_mw")), "power_mw"),
        "energy_mwh": with_fallback(_maybe_float(payload.get("energy_mwh")), "energy_mwh"),
        "emissions_t": with_fallback(
            _maybe_float(payload.get("emissions_t") or payload.get("co2_t")),
            "emissions_t",
        ),
        "demand_mw": with_fallback(_maybe_float(payload.get("demand_mw") or payload.get("demand")), "demand_mw"),
        "price_per_mwh": with_fallback(
            _maybe_float(payload.get("price_per_mwh") or payload.get("price")),
            "price_per_mwh",
        ),
        "name": payload.get("facility_name") or payload.get("name") or last_known.get("name"),
        "fuel": payload.get("fuel") or last_known.get("fuel"),
        "state": payload.get("state") or last_known.get("state"),
        "lat": with_fallback(_maybe_float(payload.get("lat")), "lat"),
        "lon": with_fallback(_maybe_float(payload.get("lon")), "lon"),
    }

    row_df = pd.DataFrame([row])

    missing_cols = [col for col in snapshot.columns if col not in row_df.columns]
    for column in missing_cols:
        row_df[column] = pd.NA
    combined = pd.concat([snapshot[snapshot["facility_id"] != facility_id], row_df], ignore_index=True, sort=False)
    return combined.sort_values("ts_event").reset_index(drop=True)


def prepare_map_source(df: pd.DataFrame) -> pd.DataFrame:
    """Return map-ready DataFrame with pydeck color/radius columns."""

    palette = {
        "battery": [255, 127, 0],
        "coal": [54, 69, 79],
        "gas": [255, 160, 122],
        "hydro": [66, 135, 245],
        "solar": [255, 215, 0],
        "wind": [46, 204, 113],
    }
    data = df.copy()
    data["fuel_norm"] = data["fuel"].str.lower().fillna("other")
    data["color"] = data["fuel_norm"].apply(lambda fuel: palette.get(fuel, [127, 140, 141]))
    data["power_display"] = data["power_mw"].fillna(0.0)
    data["power_str"] = data["power_display"].map(lambda x: f"{x:,.2f}") 
    power_scale = data["power_display"].abs().pow(0.5)  # sqrt scaling tames very large plants
    scaled = power_scale.clip(lower=0.2) * MARKER_SCALE_FACTOR
    data["radius"] = scaled.clip(lower=MARKER_MIN_RADIUS_M, upper=MARKER_MAX_RADIUS_M)
    return data.dropna(subset=["lat", "lon"])


class MQTTSubscriber:
    """Background MQTT subscriber feeding a queue."""

    def __init__(self, settings: FrontendSettings) -> None:
        self.settings = settings
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.queue: Queue = Queue()
        self._lock = threading.Lock()
        self._started = False
        self.client.enable_logger(logging.getLogger("a2.frontend.mqtt"))
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    @property
    def is_connected(self) -> bool:
        return bool(self.client.is_connected())

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self.client.connect(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
            self.client.loop_start()
            self._started = True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self.client.loop_stop()
            self.client.disconnect()
            self._started = False

    def drain(self) -> Iterable[Dict[str, Any]]:
        while True:
            try:
                yield self.queue.get_nowait()
            except Empty:
                break

    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:  # type: ignore[override]
        if getattr(reason_code, "value", reason_code) == 0:
            LOGGER.info("Connected to MQTT broker %s:%s", self.settings.mqtt_host, self.settings.mqtt_port)
            client.subscribe(self.settings.mqtt_topic)
        else:
            LOGGER.error("MQTT connection failed rc=%s", reason_code)

    def _on_disconnect(self, client: mqtt.Client, userdata, disconnect_flags, reason_code, properties=None) -> None:  # type: ignore[override]
        LOGGER.warning("MQTT disconnected rc=%s", getattr(reason_code, "value", reason_code))

    def _on_message(self, client: mqtt.Client, userdata, message) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except json.JSONDecodeError:
            LOGGER.warning("Ignoring non-JSON payload on topic %s", message.topic)
            return
        try:
            normalized = normalize_payload(payload)
        except ValueError as exc:
            LOGGER.warning("Skipping payload missing required fields (%s): %s", exc, payload)
            return
        self.queue.put(normalized)


def seed_snapshot(seed_df: pd.DataFrame) -> pd.DataFrame:
    """Reset session snapshot + events from the CSV seed."""

    snapshot = (
        seed_df.sort_values("ts_event")
        .dropna(subset=["ts_event"])
        .drop_duplicates("facility_id", keep="last")
        .reset_index(drop=True)
    )
    st.session_state.snapshot = snapshot
    st.session_state.events = []
    return snapshot


def ensure_subscriber(settings: FrontendSettings) -> MQTTSubscriber:
    """Return an active subscriber, restarting if settings changed."""

    subscriber: Optional[MQTTSubscriber] = st.session_state.get("subscriber")  # type: ignore[assignment]
    prior_settings: Optional[FrontendSettings] = st.session_state.get("subscriber_settings")  # type: ignore[assignment]
    if subscriber and prior_settings == settings:
        return subscriber

    if subscriber:
        subscriber.stop()

    subscriber = MQTTSubscriber(settings)
    subscriber.start()
    st.session_state.subscriber = subscriber
    st.session_state.subscriber_settings = settings
    return subscriber


def settings_panel(current: FrontendSettings) -> Tuple[FrontendSettings, bool]:
    """Render sidebar controls for data/mqtt sources."""

    with st.sidebar.form("settings_form"):
        st.markdown("### Data Sources")
        cache_path = st.text_input("Cache path", value=str(current.cache_path))
        mqtt_host = st.text_input("MQTT host", value=current.mqtt_host)
        mqtt_port = st.number_input("MQTT port", min_value=1, max_value=65535, value=current.mqtt_port, step=1)
        mqtt_topic = st.text_input("MQTT topic", value=current.mqtt_topic)
        refresh = st.number_input(
            "Auto refresh interval (seconds)",
            min_value=1.0,
            max_value=60.0,
            value=current.refresh_interval_s,
            step=1.0,
        )
        submitted = st.form_submit_button("Apply & reconnect")

    if not submitted:
        return current, False

    new_settings = FrontendSettings(
        cache_path=Path(cache_path).expanduser(),
        mqtt_host=mqtt_host.strip() or current.mqtt_host,
        mqtt_port=int(mqtt_port),
        mqtt_topic=mqtt_topic.strip() or current.mqtt_topic,
        refresh_interval_s=float(refresh),
    )
    return new_settings, new_settings != current


def render_dashboard() -> None:
    st.set_page_config(page_title="COMP5339 A2 Dashboard", layout="wide")
    st.title("Electricity Facilities – Live Metrics")
    st.caption("Seeded from CSV cache, live updates via MQTT.")

    if "settings" not in st.session_state:
        st.session_state.settings = parse_runtime_settings(sys.argv[1:])
    current_settings: FrontendSettings = st.session_state.settings  # type: ignore[assignment]

    settings, settings_changed = settings_panel(current_settings)
    st.session_state.settings = settings

    try:
        seed_df = load_seed_data(settings.cache_path)
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    reload_requested = st.sidebar.button("Reload cache", help="Force a fresh load from disk.")
    needs_reset = settings_changed or reload_requested or ("snapshot" not in st.session_state)

    snapshot = st.session_state.get("snapshot")
    if needs_reset or snapshot is None:
        snapshot = seed_snapshot(seed_df)
    if "events" not in st.session_state:
        st.session_state.events: List[Dict[str, Any]] = []

    try:
        subscriber = ensure_subscriber(settings)
    except Exception as exc:
        st.sidebar.error(f"MQTT subscriber failed: {exc}")
        st.stop()
        return

    status_label = "Connected" if subscriber.is_connected else "Disconnected"
    st.sidebar.markdown(f"**MQTT status:** {status_label}")
    st.sidebar.caption(
        f"Broker: {settings.mqtt_host}:{settings.mqtt_port} · Topic: {settings.mqtt_topic}"
    )

    for payload in subscriber.drain():
        st.session_state.snapshot = apply_event(st.session_state.snapshot, payload)
        enriched_payload = dict(payload)
        if isinstance(enriched_payload.get("ts_event"), pd.Timestamp):
            enriched_payload["ts_event"] = enriched_payload["ts_event"].to_pydatetime()
        st.session_state.events.insert(0, enriched_payload)
    st.session_state.events = st.session_state.events[:200]
    snapshot = st.session_state.snapshot

    fuels = sorted(snapshot["fuel"].dropna().unique()) if "fuel" in snapshot else []
    states = sorted(snapshot["state"].dropna().unique()) if "state" in snapshot else []
    selected_fuels = st.sidebar.multiselect("Fuel type", fuels, default=fuels)
    selected_states = st.sidebar.multiselect("Network region", states, default=states)
    auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)

    mask = pd.Series(True, index=snapshot.index)
    if selected_fuels:
        mask &= snapshot["fuel"].isin(selected_fuels)
    if selected_states:
        mask &= snapshot["state"].isin(selected_states)
    filtered = snapshot[mask]

    col_map, col_summary = st.columns([3, 1])
    with col_map:
        if filtered.empty:
            st.warning("No facilities match the selected filters.")
        else:
            source = prepare_map_source(filtered)
            if source.empty:
                st.warning("Facilities are missing coordinates; nothing to plot.")
            else:
                midpoint = (source["lat"].mean(), source["lon"].mean())
                st.pydeck_chart(
                    pdk.Deck(
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                        initial_view_state=pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=4.3, pitch=35),
                        layers=[
                            pdk.Layer(
                                "ScatterplotLayer",
                                data=source,
                                get_position="[lon, lat]",
                                get_fill_color="color",
                                get_radius="radius",
                                pickable=True,
                                stroked=False,
                            )
                        ],
                        tooltip={
                            "html": "<b>{name}</b><br/>Fuel: {fuel}<br/>Power: {power_str} MW<br/>CO2: {emissions_t}",
                            "style": {"backgroundColor": "steelblue", "color": "white"},
                        },
                        
                    )
                )
    with col_summary:
        st.subheader("Summary")
        st.metric("Total Power (MW)", f"{filtered['power_mw'].fillna(0.0).sum():,.2f}")
        st.metric("Total CO2 (t/5min)", f"{filtered['emissions_t'].fillna(0.0).sum():,.2f}")
        latest_ts = filtered["ts_event"].max()
        latest_str = latest_ts.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(latest_ts) else "n/a"
        st.metric("Latest event time", latest_str)

    st.subheader("Recent Events")
    if not st.session_state.events:
        st.info("Waiting for streamed events...")
    else:
        events_df = pd.DataFrame(st.session_state.events)
        columns = ["facility_id", "ts_event", "power_mw", "emissions_t", "demand_mw", "price_per_mwh"]
        existing = [col for col in columns if col in events_df.columns]
        st.dataframe(events_df[existing].head(50), use_container_width=True)

    if auto_refresh:
        time.sleep(settings.refresh_interval_s)
        rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
        if rerun:
            rerun()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    render_dashboard()


if __name__ == "__main__":
    main()
