"""Streamlit dashboard for COMP5339 Assignment 2 (Task 4).

This implementation mirrors the original Streamlit app shipped with the
assignment starter, but it now reads the CSV cache produced by the rebuilt
backend (columns such as ``power_mw``, ``energy_mwh``, ``emissions_t``) and
subscribes to MQTT using paho's v2 callback API. No environment variables are
required; defaults are defined directly in the dataclass below.

Run with::

    streamlit run a2_frontend.py --server.headless true --server.port 8501
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Dict, Iterable, List, Optional

import pandas as pd
import pydeck as pdk
import streamlit as st
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.frontend")


@dataclass
class FrontendSettings:
    """Configuration used by the dashboard."""

    cache_path: Path = Path("data/cache/nem_metrics.csv")
    mqtt_host: str = "test.mosquitto.org"
    mqtt_port: int = 1883
    mqtt_topic: str = "nem/+/+/#"
    refresh_interval_s: float = 5.0


SETTINGS = FrontendSettings()


def load_seed_data(path: Path) -> pd.DataFrame:
    """Load the backend CSV cache (raises if missing)."""
    if not path.exists():
        raise FileNotFoundError(f"Cache {path} missing. Run backend build mode first.")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Cache {path} is empty.")
    numeric = ["power_mw", "energy_mwh", "emissions_t", "demand_mw", "price_per_mwh", "lat", "lon"]
    for column in numeric:
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ts_event"] = pd.to_datetime(df["ts_event"], errors="coerce")
    df["ts_ingest"] = pd.to_datetime(df.get("ts_ingest"), errors="coerce")
    return df.sort_values("ts_event").dropna(subset=["lat", "lon"])


def _maybe_float(value: object) -> Optional[float]:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(val):
        return None
    return val


def apply_event(snapshot: pd.DataFrame, payload: Dict[str, object]) -> pd.DataFrame:
    """Merge one MQTT payload into the snapshot DataFrame."""
    facility_id = str(payload["facility_id"])
    ts_event = pd.to_datetime(payload["ts_event"], errors="coerce")
    if pd.isna(ts_event):
        return snapshot
    row = {
        "facility_id": facility_id,
        "ts_event": ts_event,
        "ts_ingest": pd.to_datetime(payload.get("ts_ingest"), errors="coerce") or datetime.utcnow(),
        "power_mw": _maybe_float(payload.get("power_mw") or payload.get("energy_mwh")),
        "energy_mwh": _maybe_float(payload.get("energy_mwh")),
        "emissions_t": _maybe_float(payload.get("emissions_t") or payload.get("co2_t")),
        "demand_mw": _maybe_float(payload.get("demand_mw") or payload.get("demand")),
        "price_per_mwh": _maybe_float(payload.get("price_per_mwh") or payload.get("price")),
        "name": payload.get("facility_name"),
        "fuel": payload.get("fuel"),
        "state": payload.get("state"),
        "lat": _maybe_float(payload.get("lat")),
        "lon": _maybe_float(payload.get("lon")),
    }
    snapshot = snapshot[snapshot["facility_id"] != facility_id]
    snapshot = pd.concat([snapshot, pd.DataFrame([row])], ignore_index=True)
    return snapshot


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
    data["radius"] = data["power_display"].abs().clip(lower=0.1) * 1200
    return data


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

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            try:
                self.client.connect(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
            except OSError as exc:
                LOGGER.error("Failed to connect to MQTT broker: %s", exc)
                raise
            self.client.loop_start()
            self._started = True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self.client.loop_stop()
            self.client.disconnect()
            self._started = False

    def drain(self) -> Iterable[Dict[str, object]]:
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
        required = {"facility_id", "ts_event"}
        if not required.issubset(payload):
            LOGGER.warning("Skipping payload missing required fields: %s", required - set(payload))
            return
        self.queue.put(payload)


def ensure_session_state(seed_df: pd.DataFrame) -> None:
    """Initialise Streamlit session_state on the first run."""
    if "snapshot" not in st.session_state:
        st.session_state.snapshot = (
            seed_df.sort_values("ts_event").drop_duplicates("facility_id", keep="last").reset_index(drop=True)
        )
    if "events" not in st.session_state:
        st.session_state.events: List[Dict[str, object]] = []
    if "subscriber" not in st.session_state:
        subscriber = MQTTSubscriber(SETTINGS)
        subscriber.start()
        st.session_state.subscriber = subscriber


def render_dashboard() -> None:
    st.set_page_config(page_title="COMP5339 A2 Dashboard", layout="wide")
    st.title("Electricity Facilities – Live Metrics")
    st.caption("Seeded from CSV cache, live updates via MQTT.")

    seed_df = load_seed_data(SETTINGS.cache_path)
    ensure_session_state(seed_df)

    snapshot = st.session_state.snapshot
    fuels = sorted(snapshot["fuel"].dropna().unique())
    states = sorted(snapshot["state"].dropna().unique())
    selected_fuels = st.sidebar.multiselect("Fuel type", fuels, default=fuels)
    selected_states = st.sidebar.multiselect("Network region", states, default=states)
    auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)

    if st.sidebar.button("Reload cache"):
        st.session_state.snapshot = load_seed_data(SETTINGS.cache_path)
        st.success("Cache reloaded.")

    subscriber: MQTTSubscriber = st.session_state.subscriber
    for payload in subscriber.drain():
        st.session_state.snapshot = apply_event(st.session_state.snapshot, payload)
        st.session_state.events.insert(0, payload)
        st.session_state.events = st.session_state.events[:200]

    filtered = st.session_state.snapshot[
        st.session_state.snapshot["fuel"].isin(selected_fuels)
        & st.session_state.snapshot["state"].isin(selected_states)
    ]

    col_map, col_summary = st.columns([3, 1])
    with col_map:
        if filtered.empty:
            st.warning("No facilities match the selected filters.")
        else:
            source = prepare_map_source(filtered)
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
                        "html": "<b>{name}</b><br/>Fuel: {fuel}<br/>Power: {power_display:.2f} MW<br/>CO₂: {emissions_t}",
                        "style": {"backgroundColor": "steelblue", "color": "white"},
                    },
                )
            )
    with col_summary:
        st.subheader("Summary")
        st.metric("Total Power (MW)", f"{filtered['power_mw'].fillna(0.0).sum():,.2f}")
        st.metric("Total CO₂ (t/5min)", f"{filtered['emissions_t'].fillna(0.0).sum():,.2f}")
        latest_ts = filtered["ts_event"].max()
        st.metric("Latest event time", latest_ts.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(latest_ts) else "n/a")

    st.subheader("Recent Events")
    if not st.session_state.events:
        st.info("Waiting for streamed events...")
    else:
        table_df = pd.DataFrame(st.session_state.events)[["facility_id", "ts_event", "power_mw", "emissions_t"]]
        st.dataframe(table_df.head(50), use_container_width=True)

    if auto_refresh:
        time.sleep(SETTINGS.refresh_interval_s)
        rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
        if rerun:
            rerun()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    try:
        render_dashboard()
    finally:
        subscriber: Optional[MQTTSubscriber] = st.session_state.get("subscriber")  # type: ignore[attr-defined]
        if subscriber:
            subscriber.stop()


if __name__ == "__main__":
    main()
