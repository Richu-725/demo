"""Streamlit dashboard for COMP5339 Assignment 2 (Task 4).

The frontend loads the latest CSV cache produced by ``a2_backend.py`` and then
listens for live MQTT updates published by the backend. Incoming events mutate
the current facility snapshot and are rendered on a pydeck scatter map, summary
panel, and recent-events table. The implementation favours clarity and keeps
state entirely inside Streamlit's ``session_state`` so the module can be
imported without side effects.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Dict, Iterable, Optional

import pandas as pd
import pydeck as pdk
import streamlit as st
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.frontend")
DEFAULT_CACHE = Path("data/cache/nem_metrics.csv")
DEFAULT_BROKER = "test.mosquitto.org"
DEFAULT_TOPIC = "nem/+/+/#"


@dataclass
class FrontendSettings:
    """Configurable knobs for the dashboard."""

    cache_path: Path = DEFAULT_CACHE
    mqtt_host: str = DEFAULT_BROKER
    mqtt_port: int = 1883
    mqtt_topic: str = DEFAULT_TOPIC
    refresh_interval_s: float = 5.0


class MQTTSubscriber:
    """Background MQTT consumer that pushes JSON payloads into a queue."""

    def __init__(self, settings: FrontendSettings) -> None:
        self.settings = settings
        self.client = self._make_client()
        self.queue: Queue[Dict[str, object]] = Queue()
        self._lock = threading.Lock()
        self._started = False

    def _make_client(self) -> mqtt.Client:
        callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api_version is not None:
            try:
                return mqtt.Client(callback_api_version=callback_api_version.VERSION2)
            except AttributeError:
                return mqtt.Client(callback_api_version=callback_api_version.VERSION1)
        return mqtt.Client()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self.client.enable_logger(logging.getLogger("a2.frontend.mqtt"))
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect
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

    def drain(self) -> Iterable[Dict[str, object]]:
        while True:
            try:
                yield self.queue.get_nowait()
            except Empty:
                break

    # MQTT callbacks -----------------------------------------------------
    def _on_connect(self, client: mqtt.Client, userdata, flags, rc, properties=None) -> None:  # type: ignore[override]
        if rc == 0:
            LOGGER.info("Connected to MQTT broker; subscribing to %s", self.settings.mqtt_topic)
            client.subscribe(self.settings.mqtt_topic)
        else:
            LOGGER.error("MQTT connection failed with rc=%s", rc)

    def _on_disconnect(self, client: mqtt.Client, userdata, rc, properties=None) -> None:  # type: ignore[override]
        LOGGER.warning("MQTT disconnected rc=%s", rc)

    def _on_message(self, client: mqtt.Client, userdata, message) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except json.JSONDecodeError:
            LOGGER.warning("Ignoring non-JSON payload on topic %s", message.topic)
            return
        required = {"facility_id", "ts_event"}
        if not required.issubset(payload):
            LOGGER.warning("Skipping payload missing required fields: %s", required - required.intersection(payload))
            return
        self.queue.put(payload)


def load_cache(cache_path: Path) -> Optional[pd.DataFrame]:
    """Load the backend cache if it exists."""
    if not cache_path.exists():
        return None
    df = pd.read_csv(cache_path)
    if df.empty:
        return None
    numeric_cols = ["power_mw", "energy_mwh", "emissions_t", "demand_mw", "price_per_mwh", "lat", "lon"]
    for column in numeric_cols:
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ts_event"] = pd.to_datetime(df["ts_event"], errors="coerce")
    return df


def apply_event(df: pd.DataFrame, payload: Dict[str, object]) -> pd.DataFrame:
    """Merge a single MQTT payload into the current snapshot."""
    facility_id = str(payload["facility_id"])
    ts_event = pd.to_datetime(payload["ts_event"], errors="coerce")
    if pd.isna(ts_event):
        return df
    row = {
        "facility_id": facility_id,
        "ts_event": ts_event,
        "ts_ingest": pd.to_datetime(payload.get("ts_ingest"), errors="coerce") or datetime.utcnow(),
        "power_mw": pd.to_numeric(payload.get("power_mw") or payload.get("energy_mwh"), errors="coerce"),
        "energy_mwh": pd.to_numeric(payload.get("energy_mwh"), errors="coerce"),
        "emissions_t": pd.to_numeric(payload.get("emissions_t") or payload.get("co2_t"), errors="coerce"),
        "demand_mw": pd.to_numeric(payload.get("demand_mw") or payload.get("demand"), errors="coerce"),
        "price_per_mwh": pd.to_numeric(payload.get("price_per_mwh") or payload.get("price"), errors="coerce"),
        "name": payload.get("facility_name"),
        "fuel": payload.get("fuel"),
        "state": payload.get("state"),
        "lat": pd.to_numeric(payload.get("lat"), errors="coerce"),
        "lon": pd.to_numeric(payload.get("lon"), errors="coerce"),
    }
    df = df[df["facility_id"] != facility_id]
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df


def prepare_map_source(df: pd.DataFrame) -> pd.DataFrame:
    """Derive map-ready columns (radius, color) from the current snapshot."""
    if df.empty:
        return df
    data = df.copy()
    data["fuel_norm"] = data["fuel"].str.lower().fillna("other")
    palette = {
        "battery": [255, 127, 0],
        "coal": [54, 69, 79],
        "gas": [255, 160, 122],
        "hydro": [66, 135, 245],
        "solar": [255, 215, 0],
        "wind": [46, 204, 113],
    }
    data["color"] = data["fuel_norm"].apply(lambda fuel: palette.get(fuel, [127, 140, 141]))
    data["power_display"] = data["power_mw"].fillna(0.0)
    data["radius"] = data["power_display"].abs().clip(lower=0.1) * 1000
    return data


def render_dashboard(settings: FrontendSettings) -> None:
    """Main Streamlit UI."""
    st.set_page_config(page_title="COMP5339 A2 Dashboard", layout="wide")
    st.title("Electricity Facilities – Live Metrics")
    st.caption("Streaming facility metrics from MQTT topics published by the backend pipeline.")

    seed_df = load_cache(settings.cache_path)
    if seed_df is None:
        st.error("Cache not found. Run the backend build mode first.")
        return

    if "current_df" not in st.session_state:
        st.session_state.current_df = seed_df.sort_values("ts_event").drop_duplicates("facility_id", keep="last")
        st.session_state.events = []

    # Sidebar filters
    fuels = sorted(st.session_state.current_df["fuel"].dropna().unique())
    states = sorted(st.session_state.current_df["state"].dropna().unique())
    selected_fuels = st.sidebar.multiselect("Fuel type", fuels, default=fuels)
    selected_states = st.sidebar.multiselect("Network region", states, default=states)
    auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
    if st.sidebar.button("Reload cache"):
        reloaded = load_cache(settings.cache_path)
        if reloaded is not None:
            st.session_state.current_df = reloaded.sort_values("ts_event").drop_duplicates("facility_id", keep="last")
            st.success("Cache reloaded.")

    # MQTT subscriber
    if "subscriber" not in st.session_state:
        subscriber = MQTTSubscriber(settings)
        try:
            subscriber.start()
        except OSError as exc:
            st.error(f"Could not connect to MQTT broker: {exc}")
            return
        st.session_state.subscriber = subscriber
    subscriber: MQTTSubscriber = st.session_state.subscriber
    new_events = list(subscriber.drain())
    for payload in new_events:
        st.session_state.current_df = apply_event(st.session_state.current_df, payload)
        st.session_state.events.insert(0, payload)
        st.session_state.events = st.session_state.events[:200]

    filtered_df = st.session_state.current_df[
        st.session_state.current_df["fuel"].isin(selected_fuels)
        & st.session_state.current_df["state"].isin(selected_states)
    ]
    map_df = prepare_map_source(filtered_df)

    col_map, col_summary = st.columns([3, 1])
    with col_map:
        if map_df.empty:
            st.warning("No facilities match the current filters.")
        else:
            midpoint = (map_df["lat"].mean(), map_df["lon"].mean())
            st.pydeck_chart(
                pdk.Deck(
                    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                    initial_view_state=pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=4.5, pitch=35),
                    layers=[
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=map_df,
                            get_position="[lon, lat]",
                            get_fill_color="color",
                            get_radius="radius",
                            pickable=True,
                            filled=True,
                            stroked=False,
                        ),
                    ],
                    tooltip={"html": "<b>{name}</b><br/>Fuel: {fuel}<br/>Power: {power_display:.2f} MW<br/>CO₂: {emissions_t}"},
                )
            )
    with col_summary:
        st.subheader("Summary")
        st.metric("Total Power (MW)", f"{filtered_df['power_mw'].fillna(0.0).sum():,.2f}")
        st.metric("Total CO₂ (t/5min)", f"{filtered_df['emissions_t'].fillna(0.0).sum():,.2f}")
        latest_ts = filtered_df["ts_event"].max()
        st.metric("Latest event time", latest_ts.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(latest_ts) else "n/a")

    st.subheader("Recent Events")
    if not st.session_state.events:
        st.info("Waiting for streamed events...")
    else:
        events_table = pd.DataFrame(st.session_state.events)[
            ["facility_id", "facility_name", "ts_event", "power_mw", "emissions_t"]
        ]
        st.dataframe(events_table.head(50), use_container_width=True)

    if auto_refresh:
        time.sleep(settings.refresh_interval_s)
        st.experimental_rerun()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    settings = FrontendSettings()
    try:
        render_dashboard(settings)
    finally:
        subscriber: Optional[MQTTSubscriber] = st.session_state.get("subscriber")  # type: ignore[attr-defined]
        if subscriber:
            subscriber.stop()


if __name__ == "__main__":
    main()
