"""
Streamlit dashboard for COMP5339 Assignment 2 (Task 4).

The app subscribes to the MQTT topics published by the backend pipeline, keeps
track of the latest metric per facility, and renders an interactive map plus
summary panels. Run with:

    streamlit run a2_frontend.py

Environment variables:
    MQTT_HOST / MQTT_PORT   - broker connection (default test.mosquitto.org:1883)
    MQTT_TOPIC              - topic filter (default nem/+/+/#)
    FRONTEND_CACHE_DIR      - directory containing backend CSV caches
    FRONTEND_REFRESH_SEC    - auto-refresh interval (default 5 seconds)

Flow overview
-------------
1. Resolve ``FrontendSettings`` from the environment and seed state from the latest backend CSV cache.
2. Spawn an ``MQTTSubscriber`` thread that listens for live metric events and buffers them in a thread-safe queue.
3. On each Streamlit rerun, drain the queue, merge new events into ``current``/``history`` tables, and update the session state.
4. Render map, summary panels, and recent events table; optionally auto-trigger a rerun after ``refresh_interval_s`` seconds.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pydeck as pdk
import streamlit as st
from pandas import Timestamp
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.frontend")


@dataclass(frozen=True)
class FrontendSettings:
    """Runtime configuration for the dashboard."""

    mqtt_host: str = "test.mosquitto.org"
    mqtt_port: int = 1883
    mqtt_topic: str = "nem/+/+/#"
    cache_dir: Path = Path("data/cache")
    refresh_interval_s: float = 5.0
    history_limit: int = 1000
    color_palette: Dict[str, List[int]] = field(
        default_factory=lambda: {
            "battery": [255, 127, 0],
            "coal": [54, 69, 79],
            "gas": [255, 160, 122],
            "hydro": [66, 135, 245],
            "solar": [255, 215, 0],
            "wind": [46, 204, 113],
            "bioenergy": [39, 174, 96],
            "other": [127, 140, 141],
        }
    )

    @staticmethod
    def from_env() -> "FrontendSettings":
        """Construct settings using environment overrides when present."""
        env = os.environ
        return FrontendSettings(
            mqtt_host=env.get("MQTT_HOST", "test.mosquitto.org"),
            mqtt_port=int(env.get("MQTT_PORT", "1883")),
            mqtt_topic=env.get("MQTT_TOPIC", "nem/+/+/#"),
            cache_dir=Path(env.get("FRONTEND_CACHE_DIR", "data/cache")),
            refresh_interval_s=float(env.get("FRONTEND_REFRESH_SEC", "5")),
        )


def _make_mqtt_client() -> mqtt.Client:
    """Create an MQTT client using the latest callback API if available."""
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        try:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION2)
        except AttributeError:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION1)
    return mqtt.Client()


def _latest_cache_path(cache_dir: Path) -> Optional[Path]:
    """Return the newest CSV cache file inside the configured cache directory."""
    if not cache_dir.exists():
        return None
    metrics_files = list(cache_dir.glob("*_metrics.csv"))
    csv_files = metrics_files if metrics_files else list(cache_dir.glob("*.csv"))
    csv_files = sorted(csv_files, key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0] if csv_files else None


def _load_seed_data(cache_dir: Path) -> Optional[pd.DataFrame]:
    """Load the most recent backend CSV cache to bootstrap dashboard state."""
    cache_path = _latest_cache_path(cache_dir)
    if not cache_path:
        return None
    seed = pd.read_csv(cache_path)
    numeric_cols = ["power_mw", "co2_t", "price", "demand", "lat", "lon"]
    for column in numeric_cols:
        if column in seed:
            seed[column] = pd.to_numeric(seed[column], errors="coerce")
    seed["ts_event"] = pd.to_datetime(seed["ts_event"], errors="coerce")
    if "ts_ingest" in seed.columns:
        seed["ts_ingest"] = pd.to_datetime(seed["ts_ingest"], errors="coerce")
    else:
        seed["ts_ingest"] = seed["ts_event"]
    return seed


def _maybe_float(value: Any) -> Optional[float]:
    """Best-effort conversion to float that tolerates pandas NA markers."""
    if value is None:
        return None
    try:
        value_float = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(value_float):
        return None
    return value_float


def _parse_timestamp(value: Any, default: Optional[datetime] = None) -> datetime:
    """Parse timestamp-like inputs into naive UTC datetimes."""
    try:
        ts = Timestamp(value)
    except (ValueError, TypeError) as exc:
        if default is not None:
            return default
        raise ValueError(f"Unable to parse timestamp from {value!r}") from exc
    if ts.tzinfo is not None:
        try:
            ts = ts.tz_convert("UTC")
        except TypeError:
            ts = ts.tz_localize("UTC")
        ts = ts.tz_localize(None)
    dt = ts.to_pydatetime()
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _normalize_event(payload: Dict[str, object]) -> Optional[Dict[str, object]]:
    """Validate raw MQTT payloads and coerce fields into consistent Python types."""
    required = {"facility_id", "ts_event"}
    if not required.issubset(payload):
        missing = required.difference(payload)
        LOGGER.warning("Skipping payload missing required fields: %s", missing)
        return None
    try:
        ts_event = _parse_timestamp(payload["ts_event"])
    except ValueError as exc:
        LOGGER.warning("Skipping payload with invalid ts_event %s (%s)", payload["ts_event"], exc)
        return None
    ts_ingest_raw = payload.get("ts_ingest", datetime.utcnow().isoformat())
    ts_ingest = _parse_timestamp(ts_ingest_raw, default=datetime.utcnow())
    normalized = {
        "facility_id": str(payload["facility_id"]),
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "power_mw": _maybe_float(payload.get("power_mw")),
        "co2_t": _maybe_float(payload.get("co2_t")),
        "price": _maybe_float(payload.get("price")),
        "demand": _maybe_float(payload.get("demand")),
        "raw": payload,
    }
    return normalized


class MQTTSubscriber:
    """Background MQTT subscriber pushing decoded events into a queue."""

    def __init__(self, settings: FrontendSettings) -> None:
        """Configure the MQTT client and shared state using the provided settings."""
        self.settings = settings
        self.logger = logging.getLogger("a2.frontend.mqtt")
        self.client = _make_mqtt_client()
        self.queue: Queue[Dict[str, object]] = Queue()
        self._lock = threading.Lock()
        self._started = False
        self.connected = threading.Event()

        self.client.enable_logger(self.logger)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def start(self) -> None:
        """Establish a connection to the broker and start the background network loop."""
        with self._lock:
            if self._started:
                return
            self.logger.info(
                "Connecting to MQTT broker host=%s port=%s topic=%s",
                self.settings.mqtt_host,
                self.settings.mqtt_port,
                self.settings.mqtt_topic,
            )
            try:
                self.client.connect(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
            except OSError as exc:
                self.logger.error("Failed to connect to MQTT broker: %s", exc)
                raise
            self.client.loop_start()
            self._started = True

    def stop(self) -> None:
        """Stop the network loop and drop the broker connection."""
        with self._lock:
            if not self._started:
                return
            self.client.loop_stop()
            self.client.disconnect()
            self._started = False

    def drain(self) -> Iterable[Dict[str, object]]:
        """Yield all queued events without blocking."""
        while True:
            try:
                yield self.queue.get_nowait()
            except Empty:
                break

    # MQTT callbacks -----------------------------------------------------
    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:  # type: ignore[override]
        """Subscribe to the configured topic once the broker acknowledges the connection."""
        code = getattr(reason_code, "value", reason_code)
        if code == 0:
            self.logger.info("MQTT connected; subscribing to %s", self.settings.mqtt_topic)
            client.subscribe(self.settings.mqtt_topic)
            self.connected.set()
        else:
            self.logger.error("MQTT connection failed with rc=%s", code)

    def _on_disconnect(self, client: mqtt.Client, userdata, reason_code, properties=None) -> None:  # type: ignore[override]
        """Handle broker disconnects by logging and clearing the ready flag."""
        code = getattr(reason_code, "value", reason_code)
        self.logger.warning("MQTT disconnected rc=%s", code)
        self.connected.clear()

    def _on_message(self, client: mqtt.Client, userdata, message) -> None:
        """Decode JSON payloads, normalise them, and push onto the shared queue."""
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except json.JSONDecodeError:
            self.logger.warning("Received non-JSON payload on topic %s", message.topic)
            return
        payload["topic"] = message.topic
        normalized = _normalize_event(payload)
        if normalized is not None:
            self.queue.put(normalized)


def _initial_state(seed_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split the seed cache into facility metadata and the latest per-facility metrics."""
    columns = [
        "facility_id",
        "name",
        "fuel",
        "state",
        "lat",
        "lon",
        "power_mw",
        "co2_t",
        "price",
        "demand",
        "ts_event",
        "ts_ingest",
    ]
    metadata = (
        seed_df[["facility_id", "name", "fuel", "state", "lat", "lon"]]
        .drop_duplicates("facility_id")
        .set_index("facility_id")
    )
    for col in ("capacity_mw", "status"):
        if col not in metadata.columns:
            metadata[col] = pd.NA
    latest_metrics_df = (
        seed_df.sort_values("ts_event")
        .groupby("facility_id")
        .tail(1)
        .set_index("facility_id")
    )
    for column in columns[1:]:
        if column not in latest_metrics_df.columns:
            latest_metrics_df[column] = pd.NA
    latest_metrics_df = latest_metrics_df[columns[1:]]
    latest_metrics_df.index.name = "facility_id"
    return metadata, latest_metrics_df.reset_index()[columns]


def _color_for_fuel(fuel: str, palette: Dict[str, List[int]]) -> List[int]:
    """Pick a RGB color for the supplied fuel type, falling back to 'other'."""
    if not isinstance(fuel, str):
        return palette["other"]
    return palette.get(fuel.lower(), palette["other"])


def _prepare_map_source(current_df: pd.DataFrame, palette: Dict[str, List[int]]) -> pd.DataFrame:
    """Derive map-ready columns including colors, radii, and joined metadata."""
    if current_df.empty:
        return current_df
    working = current_df.copy()
    working["fuel_norm"] = working["fuel"].str.lower().fillna("other")
    working["color"] = working["fuel_norm"].apply(lambda value: _color_for_fuel(value, palette))
    working["power_display"] = working["power_mw"].fillna(0.0)
    working["radius"] = working["power_display"].clip(lower=0.05) * 5000
    if "metadata" in st.session_state:
        meta = (
            st.session_state.metadata.reset_index()[["facility_id", "capacity_mw", "status"]]
            .fillna({"status": "unknown"})
        )
        working = working.merge(meta, on="facility_id", how="left")
    else:
        working["capacity_mw"] = pd.NA
        working["status"] = pd.NA
    working["status"] = working["status"].fillna("unknown")
    return working


def _apply_events(
    events: Iterable[Dict[str, object]],
    metadata: pd.DataFrame,
    current_df: pd.DataFrame,
    history_df: pd.DataFrame,
    history_limit: int,
) -> None:
    """Merge incoming events into the `current` snapshot and append to history with bounds."""
    columns = list(current_df.columns)
    for event in events:
        facility_id = str(event["facility_id"])
        if facility_id not in metadata.index:
            LOGGER.warning("Skipping event for unknown facility %s", facility_id)
            continue
        row_meta = metadata.loc[facility_id]
        updated: Dict[str, Any] = {
            "facility_id": facility_id,
            "name": row_meta["name"],
            "fuel": row_meta["fuel"],
            "state": row_meta["state"],
            "lat": row_meta["lat"],
            "lon": row_meta["lon"],
            "power_mw": event["power_mw"],
            "co2_t": event["co2_t"],
            "price": event["price"],
            "demand": event["demand"],
            "ts_event": event["ts_event"],
            "ts_ingest": event["ts_ingest"],
        }
        update_df = pd.DataFrame([updated], columns=columns)
        mask = current_df["facility_id"] == facility_id
        if mask.any():
            current_df.loc[mask, columns] = update_df.to_numpy()
        else:
            current_df.loc[len(current_df), columns] = update_df.to_numpy()[0]
        history_df.loc[len(history_df), columns] = update_df.to_numpy()[0]
    if len(history_df) > history_limit:
        excess = len(history_df) - history_limit
        history_df.drop(history_df.index[:excess], inplace=True)
        history_df.reset_index(drop=True, inplace=True)


def _render_summary(current_df: pd.DataFrame) -> None:
    """Render headline metrics summarising total power and emissions."""
    total_power = current_df["power_mw"].fillna(0.0).sum()
    total_co2 = current_df["co2_t"].fillna(0.0).sum()
    st.metric("Total Power (MW)", f"{total_power:,.2f}")
    st.metric("Total CO₂ (t/5min)", f"{total_co2:,.2f}")


def _render_map(map_df: pd.DataFrame) -> None:
    """Display the facilities on a PyDeck scatter map with size and colour cues."""
    if map_df.empty:
        st.info("No facility data available yet. Waiting for events...")
        return
    center_lat = map_df["lat"].mean()
    center_lon = map_df["lon"].mean()
    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=4, pitch=20)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius="radius",
        get_line_color=[255, 255, 255],
        line_width_min_pixels=0,
        pickable=True,
    )
    tooltip_dict: Dict[str, Any] = {
        "html": "<b>{name}</b><br/>Fuel: {fuel}<br/>Status: {status}<br/>Capacity: {capacity_mw}<br/>Power: {power_display:.2f} MW<br/>CO₂: {co2_t}",
        "style": {"backgroundColor": "steelblue", "color": "white"},
    }
    deck = pdk.Deck(
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        initial_view_state=view_state,
        layers=[layer],
        tooltip=tooltip_dict,  # type: ignore[arg-type]
    )
    st.pydeck_chart(deck)


def _sidebar_filters(current_df: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar controls and return the filtered facility dataframe."""
    sidebar = st.sidebar
    sidebar.header("Filters")
    fuels = sorted([fuel for fuel in current_df["fuel"].dropna().unique()])
    states = sorted([state for state in current_df["state"].dropna().unique()])
    selected_fuels = sidebar.multiselect("Fuel type", fuels, default=fuels)
    selected_states = sidebar.multiselect("Network region", states, default=states)
    filtered = current_df[
        current_df["fuel"].isin(selected_fuels) & current_df["state"].isin(selected_states)
    ]
    return filtered


def run_app() -> None:
    """
    Launch the Streamlit UI, hydrate state from cached data, and stream live updates.

    Steps:
        1. Load ``FrontendSettings`` (allowing overrides via env vars) and configure Streamlit.
        2. Prime session state from the latest backend cache so facility metadata is available immediately.
        3. Start the background ``MQTTSubscriber`` thread to pull metric events into an in-memory queue.
        4. Merge queued events into the current/history tables, render the map + summary widgets, and schedule reruns
           while auto-refresh is enabled.
    """
    settings = FrontendSettings.from_env()
    st.set_page_config(page_title="COMP5339 A2 Dashboard", layout="wide")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    st.title("Electricity Facilities – Live Metrics")
    st.caption(
        "Streaming facility metrics from MQTT topics published by the backend pipeline. "
        "Use the sidebar to filter by region or fuel type."
    )

    seed_df = _load_seed_data(settings.cache_dir)
    if seed_df is None or seed_df.empty:
        st.error(
            "No cached data found. Run `python a2_backend.py --mode build` first so the frontend "
            "can bootstrap facility metadata."
        )
        return

    if "metadata" not in st.session_state:
        metadata, current_df = _initial_state(seed_df)
        st.session_state.metadata = metadata
        st.session_state.current_df = current_df
        st.session_state.history_df = pd.DataFrame(columns=current_df.columns)
        st.session_state.events_received = 0

    if "subscriber" not in st.session_state:
        try:
            subscriber = MQTTSubscriber(settings)
            subscriber.start()
        except OSError as exc:
            st.error(f"Could not connect to MQTT broker: {exc}")
            return
        st.session_state.subscriber = subscriber

    subscriber: MQTTSubscriber = st.session_state.subscriber
    metadata: pd.DataFrame = st.session_state.metadata
    current_df: pd.DataFrame = st.session_state.current_df
    history_df: pd.DataFrame = st.session_state.history_df

    new_events = list(subscriber.drain())
    if new_events:
        _apply_events(
            events=new_events,
            metadata=metadata,
            current_df=current_df,
            history_df=history_df,
            history_limit=settings.history_limit,
        )
        st.session_state.events_received += len(new_events)
        st.session_state.current_df = current_df
        st.session_state.history_df = history_df

    filtered_current = _sidebar_filters(current_df)
    map_source = _prepare_map_source(filtered_current, settings.color_palette)

    col_left, col_right = st.columns([2, 1])
    with col_left:
        _render_map(map_source)
    with col_right:
        st.subheader("Summary")
        _render_summary(filtered_current)
        st.metric("Events received", f"{st.session_state.events_received}")
        if not filtered_current.empty:
            latest_ts = filtered_current["ts_event"].max()
            st.metric("Latest event time", latest_ts.strftime("%Y-%m-%d %H:%M:%S"))

    st.subheader("Recent Events")
    if history_df.empty:
        st.info("Waiting for streamed events...")
    else:
        st.dataframe(
            history_df.sort_values("ts_event", ascending=False)
            .head(50)
            .reset_index(drop=True)[
                ["facility_id", "name", "fuel", "state", "power_mw", "co2_t", "ts_event", "ts_ingest"]
            ],
            use_container_width=True,
        )

    st.sidebar.subheader("Connection")
    st.sidebar.write(f"Broker: `{settings.mqtt_host}:{settings.mqtt_port}`")
    st.sidebar.write(f"Topic filter: `{settings.mqtt_topic}`")
    auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
    if st.sidebar.button("Reload cache"):
        seed_df_latest = _load_seed_data(settings.cache_dir)
        if seed_df_latest is not None:
            metadata, current_df_new = _initial_state(seed_df_latest)
            st.session_state.metadata = metadata
            st.session_state.current_df = current_df_new
            st.session_state.history_df = pd.DataFrame(columns=current_df_new.columns)
            st.success("Reloaded cache successfully.")
            _safe_rerun()

    if auto_refresh:
        time.sleep(settings.refresh_interval_s)
        _safe_rerun()


def _safe_rerun() -> None:
    """Call Streamlit's rerun API in a version-safe manner."""
    rerun_fn = getattr(st, "rerun", None)
    if callable(rerun_fn):
        rerun_fn()
        return
    experimental_rerun = getattr(st, "experimental_rerun", None)
    if callable(experimental_rerun):
        experimental_rerun()


if __name__ == "__main__":
    run_app()
