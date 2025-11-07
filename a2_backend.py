"""Minimal backend pipeline for COMP5339 Assignment 2.

The backend is responsible for:
1. Fetching OpenElectricity facility metadata and metric time-series.
2. Materialising a single CSV cache (one row per facility+timestamp) that grows over time.
3. Publishing new cache rows over MQTT with strict ordering and watermarking.
4. Running in build/stream/loop modes exactly as required by the assignment brief.

Run ``python a2_backend.py --mode build`` to backfill the CSV cache,
``python a2_backend.py --mode stream`` to publish unseen rows from the CSV cache,
or ``python a2_backend.py --mode loop`` to continuously stream → backfill → sleep.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.backend")
BASE_URL = "https://api.openelectricity.org.au"
FACILITIES_ENDPOINT = "/v1/facilities/"
DATA_ENDPOINT_TEMPLATE = "/v4/data/facilities/{network}"
REQUEST_TIMEOUT = 60
REQUEST_MIN_INTERVAL = 0.25  # seconds
SERIES_COLUMN_MAP = {
    "power": "power_mw",
    "emissions": "co2_t",
    "price": "price",
    "demand": "demand",
}
MANIFEST_FILENAME = "manifest.csv"


# ---------------------------------------------------------------------------
# HTTP helpers


class RequestBudget:
    """Track the remaining API request allowance."""

    def __init__(self, limit: int, *, warn_threshold: float = 0.1) -> None:
        self.limit = max(limit, 0)
        self.used = 0
        self.warn_threshold = max(0.0, min(1.0, warn_threshold))
        self._warned = False

    @property
    def remaining(self) -> int:
        """Number of requests still available before hitting the limit."""
        return max(self.limit - self.used, 0)

    def consume(self, amount: int = 1) -> None:
        """Record request usage and raise if the allowance would be exceeded."""
        if self.used + amount > self.limit:
            raise RuntimeError("Request budget exceeded; aborting to stay within daily limits.")
        self.used += amount
        if (
            self.warn_threshold > 0
            and not self._warned
            and self.limit > 0
            and self.remaining <= self.limit * self.warn_threshold
        ):
            LOGGER.warning(
                "API request budget low: remaining=%s limit=%s threshold=%.0f%%",
                self.remaining,
                self.limit,
                self.warn_threshold * 100,
            )
            self._warned = True


class OpenElectricityClient:
    """HTTP client with pacing, retries, and budget tracking."""

    def __init__(self, api_key: str, *, request_budget: int, alert_threshold: float = 0.1) -> None:
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})
        self._budget = RequestBudget(request_budget, warn_threshold=alert_threshold)
        self._last_request_ts = 0.0

    @property
    def remaining_budget(self) -> int:
        """Expose the remaining budget for callers that need to stop early."""
        return self._budget.remaining

    def _pace(self) -> None:
        """Sleep just enough to respect the minimum interval between requests."""
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < REQUEST_MIN_INTERVAL:
            time.sleep(REQUEST_MIN_INTERVAL - elapsed)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Sequence[Tuple[str, Any]]] = None,
    ) -> requests.Response:
        """Perform an HTTP request with retry/backoff handling."""
        self._budget.consume()
        self._pace()
        url = f"{BASE_URL}{path}"
        backoff = 1.0
        for attempt in range(5):
            response = self._session.request(method, url, params=params, timeout=REQUEST_TIMEOUT)
            self._last_request_ts = time.monotonic()
            if response.status_code < 400:
                LOGGER.debug(
                    "HTTP %s %s -> %s (remaining budget=%s)",
                    method,
                    url,
                    response.status_code,
                    self._budget.remaining,
                )
                return response
            if response.status_code in {429, 500, 502, 503, 504}:
                LOGGER.warning(
                    "HTTP %s for %s %s (attempt %s) - retrying after %.1fs",
                    response.status_code,
                    method,
                    url,
                    attempt + 1,
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue
            response.raise_for_status()
        response.raise_for_status()
        return response

    def get_json(self, path: str, *, params: Optional[Sequence[Tuple[str, Any]]] = None) -> Dict[str, Any]:
        """Issue a GET request and return parsed JSON."""
        response = self._request("GET", path, params=params)
        return response.json()

    def close(self) -> None:
        """Close the underlying requests session and log usage."""
        if self._budget.used > 0:
            LOGGER.info(
                "API usage summary: used=%s remaining=%s limit=%s",
                self._budget.used,
                self._budget.remaining,
                self._budget.limit,
            )
        self._session.close()

    def __enter__(self) -> "OpenElectricityClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()


# ---------------------------------------------------------------------------
# Settings & parsing


def _parse_nullable_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 timestamps when provided."""
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _parse_interval(value: str) -> timedelta:
    """Convert a compact interval string (e.g. 5m, 1h) into timedelta."""
    if not value:
        raise ValueError("Interval string is required.")
    match = re.fullmatch(r"(?P<amount>\d+)(?P<unit>[smhd])", value.strip().lower())
    if not match:
        raise ValueError(f"Unsupported interval format '{value}'. Use digits followed by s/m/h/d.")
    amount = int(match.group("amount"))
    unit = match.group("unit")
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


@dataclass(frozen=True)
class Settings:
    """Runtime configuration resolved from environment variables and CLI overrides."""

    api_key: str
    network: str = "NEM"
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    cache_dir: Path = field(default_factory=lambda: Path("data/cache"))
    request_budget: int = 450
    request_budget_alert_threshold: float = 0.1
    metrics: Tuple[str, ...] = ("power", "emissions")
    interval: str = "5m"
    window_hours: int = 24
    loop_delay: float = 60.0
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_keepalive: int = 60
    refresh_facilities: bool = False

    @staticmethod
    def from_env() -> "Settings":
        """Construct settings using environment variables."""
        env = os.environ
        api_key = env.get("OE_API_KEY") or env.get("A2_API_KEY")
        if not api_key:
            raise RuntimeError("Missing OE_API_KEY (or A2_API_KEY) for OpenElectricity access.")
        metrics_env = env.get("A2_METRICS")
        if metrics_env:
            metrics = tuple(part.strip() for part in metrics_env.split(",") if part.strip())
        else:
            metrics = ("power", "emissions")
        invalid = [m for m in metrics if m not in SERIES_COLUMN_MAP]
        if invalid:
            raise ValueError(f"Unsupported metrics requested: {invalid}")
        start = _parse_nullable_datetime(env.get("A2_START"))
        end = _parse_nullable_datetime(env.get("A2_END"))
        interval = env.get("A2_INTERVAL", "5m")
        loop_delay = max(float(env.get("A2_LOOP_DELAY", "60")), 60.0)
        window_hours = max(int(env.get("A2_WINDOW_HOURS", "24")), 1)
        refresh_facilities = env.get("A2_REFRESH_FACILITIES", "0") in {"1", "true", "True"}

        return Settings(
            api_key=api_key,
            network=env.get("A2_NETWORK", "NEM"),
            start=start,
            end=end,
            cache_dir=Path(env.get("A2_CACHE_DIR", "data/cache")),
            request_budget=int(env.get("A2_REQUEST_BUDGET", "450")),
            request_budget_alert_threshold=float(env.get("A2_BUDGET_ALERT_THRESHOLD", "0.1")),
            metrics=metrics,
            interval=interval,
            window_hours=window_hours,
            loop_delay=loop_delay,
            mqtt_host=env.get("MQTT_HOST", "localhost"),
            mqtt_port=int(env.get("MQTT_PORT", "1883")),
            mqtt_keepalive=int(env.get("MQTT_KEEPALIVE", "60")),
            refresh_facilities=refresh_facilities,
        )

    def with_overrides(self, **kwargs: object) -> "Settings":
        """Return a copy with updated attributes."""
        return Settings(**{**self.__dict__, **kwargs})

    @property
    def facilities_path(self) -> Path:
        """Location of the cached facilities metadata."""
        return self.cache_dir / "facilities.csv"

    @property
    def metrics_path(self) -> Path:
        """Location of the growing CSV metrics cache."""
        return self.cache_dir / f"{self.network.lower()}_metrics.csv"

    @property
    def manifest_path(self) -> Path:
        """Location of manifest.csv."""
        return self.cache_dir / MANIFEST_FILENAME

    @property
    def publish_offsets_path(self) -> Path:
        """Location of the watermark used by stream mode."""
        return self.cache_dir / "publish_offsets.json"


# ---------------------------------------------------------------------------
# Domain models


@dataclass(frozen=True)
class FacilityPoint:
    """Reference metadata for a single electricity facility."""

    facility_id: str
    name: str
    fuel: str
    state: str
    lat: float
    lon: float


@dataclass(frozen=True)
class MetricEvent:
    """Published measurement for an electricity facility."""

    facility_id: str
    ts_event: datetime
    ts_ingest: datetime
    power_mw: Optional[float] = None
    co2_t: Optional[float] = None
    price: Optional[float] = None
    demand: Optional[float] = None

    def to_json(self) -> str:
        """Serialize the event to JSON."""
        payload = {
            "facility_id": self.facility_id,
            "ts_event": self.ts_event.isoformat(),
            "ts_ingest": self.ts_ingest.isoformat(),
            "power_mw": self.power_mw,
            "co2_t": self.co2_t,
            "price": self.price,
            "demand": self.demand,
        }
        return json.dumps(payload, separators=(",", ":"))


def topic_for(facility: FacilityPoint) -> str:
    """Return the MQTT topic for a facility."""
    return f"nem/{facility.state.lower()}/{facility.fuel.lower()}/{facility.facility_id.lower()}"


# ---------------------------------------------------------------------------
# Data retrieval & shaping


def discover_facilities(client: OpenElectricityClient, network: str) -> pd.DataFrame:
    """Fetch facility metadata for the requested network."""
    params: List[Tuple[str, Any]] = [("network_id", network)]
    payload = client.get_json(FACILITIES_ENDPOINT, params=params)
    facilities = payload.get("data", [])
    rows: List[Dict[str, Any]] = []
    for facility in facilities:
        code = facility.get("code")
        if not code:
            continue
        location = facility.get("location") or {}
        units = facility.get("units") or []
        fuels = sorted({unit.get("fueltech_id") for unit in units if unit.get("fueltech_id")})
        fuel = fuels[0] if fuels else "unknown"
        state = (facility.get("network_region") or "??")[:2]
        rows.append(
            {
                "facility_id": code,
                "name": facility.get("name"),
                "fuel": fuel,
                "state": state,
                "lat": location.get("lat"),
                "lon": location.get("lng"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No facilities returned for network '{network}'.")
    if df["facility_id"].isna().any():
        missing = df.loc[df["facility_id"].isna()]
        raise RuntimeError(f"Facility metadata contains missing facility_id values: {missing}")
    bounds_mask = (df["lat"].between(-45, -10)) & (df["lon"].between(110, 160))
    df = df[bounds_mask].dropna(subset=["lat", "lon"]).sort_values("facility_id").reset_index(drop=True)
    return df


def load_facilities(settings: Settings, client: OpenElectricityClient) -> pd.DataFrame:
    """Load cached facilities metadata, refreshing from the API when required."""
    cache_path = settings.facilities_path
    if cache_path.exists() and not settings.refresh_facilities:
        facilities = pd.read_csv(cache_path)
        LOGGER.info("Loaded facilities metadata from %s", cache_path)
    else:
        facilities = discover_facilities(client, settings.network)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        facilities.to_csv(cache_path, index=False)
        LOGGER.info("Fetched %s facilities and wrote %s", len(facilities), cache_path)
    required_cols = {"facility_id", "name", "fuel", "state", "lat", "lon"}
    missing = required_cols.difference(facilities.columns)
    if missing:
        raise RuntimeError(f"Facilities cache missing required columns: {missing}")
    facilities = facilities.dropna(subset=["facility_id", "lat", "lon"]).copy()
    facilities["lat"] = pd.to_numeric(facilities["lat"], errors="coerce")
    facilities["lon"] = pd.to_numeric(facilities["lon"], errors="coerce")
    return facilities


def _format_timestamp(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def fetch_timeseries(
    client: OpenElectricityClient,
    network: str,
    facility_id: str,
    metrics: Sequence[str],
    start: datetime,
    end: datetime,
    interval: str = "5m",
) -> pd.DataFrame:
    """Retrieve per-facility metric time-series."""
    params: List[Tuple[str, Any]] = []
    for metric in metrics:
        params.append(("metrics", metric))
    params.extend(
        [
            ("facility_code", facility_id),
            ("date_start", _format_timestamp(start)),
            ("date_end", _format_timestamp(end)),
            ("interval", interval),
        ]
    )
    try:
        payload = client.get_json(DATA_ENDPOINT_TEMPLATE.format(network=network), params=params)
    except requests.HTTPError as exc:  # type: ignore[attr-defined]
        status = getattr(exc.response, "status_code", None)
        if status == 416:
            LOGGER.info("phase=fetch facility=%s status=no_data", facility_id)
            return pd.DataFrame(columns=["facility_id", "series", "unit_code", "ts", "value"])
        raise
    rows: List[Dict[str, Any]] = []
    for block in payload.get("data", []):
        series = block.get("metric")
        for result in block.get("results", []):
            unit_code = (result.get("columns") or {}).get("unit_code")
            for entry in result.get("data", []):
                ts_raw, value = entry
                ts_event = datetime.fromisoformat(ts_raw)
                if ts_event.tzinfo is not None:
                    ts_event = ts_event.replace(tzinfo=None)
                rows.append(
                    {
                        "facility_id": facility_id,
                        "series": series,
                        "unit_code": unit_code,
                        "ts": ts_event,
                        "value": value,
                    }
                )
    tidy = pd.DataFrame(rows, columns=["facility_id", "series", "unit_code", "ts", "value"])
    return tidy.sort_values(["facility_id", "series", "unit_code", "ts"]).reset_index(drop=True)


def reconcile_units(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate unit-level data to facility+series+timestamp."""
    if df.empty:
        return pd.DataFrame(columns=["facility_id", "series", "ts_event", "metric_value"])
    grouped = (
        df.groupby(["facility_id", "series", "ts"], dropna=False)["value"]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"value": "metric_value", "ts": "ts_event"})
    )
    grouped = grouped.sort_values(["facility_id", "series", "ts_event"]).reset_index(drop=True)
    return grouped


def pivot_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long metric table into wide facility/timestamp rows."""
    if df.empty:
        columns = ["facility_id", "ts_event"] + list(SERIES_COLUMN_MAP.values())
        return pd.DataFrame(columns=columns)
    pivot = (
        df.pivot_table(index=["facility_id", "ts_event"], columns="series", values="metric_value", aggfunc="first")
        .reset_index()
    )
    pivot.columns.name = None
    for series_name, column in SERIES_COLUMN_MAP.items():
        if series_name in pivot:
            pivot = pivot.rename(columns={series_name: column})
        else:
            pivot[column] = pd.NA
    pivot = pivot[["facility_id", "ts_event", *SERIES_COLUMN_MAP.values()]].sort_values(["facility_id", "ts_event"])
    return pivot.reset_index(drop=True)


def attach_facility_meta(metrics_df: pd.DataFrame, facilities_df: pd.DataFrame) -> pd.DataFrame:
    """Join facility metadata into the metric dataset."""
    if metrics_df.empty:
        cols = ["facility_id", "ts_event", *SERIES_COLUMN_MAP.values(), "name", "fuel", "state", "lat", "lon"]
        return pd.DataFrame(columns=cols)
    merged = metrics_df.merge(facilities_df, how="left", on="facility_id", validate="many_to_one")
    missing = merged[merged["name"].isna()]["facility_id"].unique().tolist()
    if missing:
        raise RuntimeError(f"Missing metadata for facilities: {missing}")
    return merged


# ---------------------------------------------------------------------------
# Cache helpers


def _compute_md5(path: Path) -> str:
    """Return the hexadecimal MD5 digest of a file's contents."""
    import hashlib

    digest = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def append_csv_cache(
    df: pd.DataFrame,
    path: Path,
    *,
    keys: Tuple[str, ...] = ("facility_id", "ts_event"),
) -> Tuple[bool, Optional[str]]:
    """Append-and-dedupe a metrics DataFrame onto the canonical CSV cache."""
    if df.empty:
        md5 = _compute_md5(path) if path.exists() else None
        return False, md5
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = df.copy()
    normalized["ts_event"] = pd.to_datetime(normalized["ts_event"], errors="coerce")
    normalized = normalized.dropna(subset=["ts_event"])
    columns = [
        "facility_id",
        "ts_event",
        *SERIES_COLUMN_MAP.values(),
        "name",
        "fuel",
        "state",
        "lat",
        "lon",
    ]
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized = normalized[columns]
    if path.exists():
        existing = pd.read_csv(path, parse_dates=["ts_event"])
        combined = pd.concat([existing, normalized], ignore_index=True)
    else:
        combined = normalized
    combined = (
        combined.drop_duplicates(subset=list(keys), keep="last")
        .sort_values(list(keys))
        .reset_index(drop=True)
    )
    combined["ts_event"] = combined["ts_event"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    combined.to_csv(tmp_path, index=False)
    new_md5 = _compute_md5(tmp_path)
    existing_md5 = _compute_md5(path) if path.exists() else None
    if existing_md5 == new_md5:
        tmp_path.unlink(missing_ok=True)
        LOGGER.info("append_csv_cache: no new rows; cache unchanged.")
        return False, new_md5
    tmp_path.replace(path)
    LOGGER.info("append_csv_cache: cache now has %s rows", len(combined))
    return True, new_md5


def _update_manifest(
    manifest_path: Path,
    *,
    resource_id: str,
    cache_path: Path,
    md5: Optional[str],
    retrieved_at: datetime,
    unchanged: bool,
) -> None:
    """Record the cache write in manifest.csv."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "resource_id": resource_id,
        "cache_path": str(cache_path),
        "retrieved_at": retrieved_at.isoformat(),
        "bytes": cache_path.stat().st_size if cache_path.exists() else 0,
        "md5": md5 or "",
        "status": "unchanged" if unchanged else "updated",
    }
    manifest_df = pd.read_csv(manifest_path) if manifest_path.exists() else pd.DataFrame(columns=list(entry.keys()))
    manifest_df = manifest_df[manifest_df["resource_id"] != resource_id]
    manifest_df = pd.concat([manifest_df, pd.DataFrame([entry])], ignore_index=True)
    manifest_df.to_csv(manifest_path, index=False)


def ensure_data_dirs(paths: Iterable[Path]) -> None:
    """Create required directories if they do not exist."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# MQTT helpers & watermarking


def _maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _make_mqtt_client() -> mqtt.Client:
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        try:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION2)
        except AttributeError:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION1)
    return mqtt.Client()


def _facility_points_from_df(df: pd.DataFrame) -> Dict[str, FacilityPoint]:
    required = {"facility_id", "name", "fuel", "state", "lat", "lon"}
    if not required.issubset(df.columns):
        missing = required.difference(df.columns)
        raise ValueError(f"Missing required metadata columns: {missing}")
    deduped = df[list(required)].drop_duplicates("facility_id")
    records = deduped.to_dict(orient="records")
    points: Dict[str, FacilityPoint] = {}
    for record in records:
        facility_id = str(record["facility_id"])
        points[facility_id] = FacilityPoint(
            facility_id=facility_id,
            name=str(record["name"]),
            fuel=str(record["fuel"]),
            state=str(record["state"]),
            lat=float(record["lat"]),
            lon=float(record["lon"]),
        )
    return points


def _get_row_value(row: Any, name: str) -> Any:
    return getattr(row, name) if hasattr(row, name) else row[name]


def _event_from_row(row: Any, *, ts_ingest: Optional[datetime] = None) -> MetricEvent:
    ts_event = pd.to_datetime(_get_row_value(row, "ts_event")).to_pydatetime()
    facility_id = str(_get_row_value(row, "facility_id"))

    def _optional_float(column: str) -> Optional[float]:
        try:
            return _maybe_float(_get_row_value(row, column))
        except (AttributeError, KeyError, IndexError):
            return None

    return MetricEvent(
        facility_id=facility_id,
        ts_event=ts_event,
        ts_ingest=ts_ingest or datetime.utcnow(),
        power_mw=_optional_float("power_mw"),
        co2_t=_optional_float("co2_t"),
        price=_optional_float("price"),
        demand=_optional_float("demand"),
    )


def publish_events(
    metrics_df: pd.DataFrame,
    facility_lookup: Dict[str, FacilityPoint],
    *,
    host: str,
    port: int,
    keepalive: int,
    delay_s: float = 0.1,
    on_published: Optional[Callable[[MetricEvent], None]] = None,
) -> None:
    """Publish metric events to MQTT."""
    client = _make_mqtt_client()
    client.enable_logger(logging.getLogger("a2.backend.mqtt"))
    client.connect(host, port, keepalive=keepalive)
    client.loop_start()
    try:
        for row in metrics_df.itertuples(index=False):
            event = _event_from_row(row)
            facility = facility_lookup.get(event.facility_id)
            if facility is None:
                LOGGER.debug("Skipping publish for unknown facility %s", event.facility_id)
                continue
            topic = topic_for(facility)
            client.publish(topic, event.to_json(), qos=0)
            LOGGER.info("Published topic=%s payload=%s", topic, event.to_json())
            if on_published is not None:
                on_published(event)
            time.sleep(max(delay_s, 0.1))
    finally:
        client.loop_stop()
        client.disconnect()


def load_publish_offset(path: Path) -> Optional[datetime]:
    """Load the last published ts_event watermark."""
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    ts_raw = payload.get("last_ts_event")
    if not ts_raw:
        return None
    return datetime.fromisoformat(ts_raw)


def save_publish_offset(path: Path, ts_event: datetime) -> None:
    """Persist the latest published ts_event atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps({"last_ts_event": ts_event.isoformat()}))
    tmp_path.replace(path)


# ---------------------------------------------------------------------------
# Pipeline orchestration


def backfill_until_empty(settings: Settings, client: OpenElectricityClient, facilities_df: pd.DataFrame) -> Path:
    """Extend the CSV cache by iterating fixed windows until the API stops returning data."""
    csv_path = settings.metrics_path
    interval_delta = _parse_interval(settings.interval)
    if csv_path.exists():
        cached = pd.read_csv(csv_path, parse_dates=["ts_event"])
        last_ts = cached["ts_event"].max() if not cached.empty else None
    else:
        last_ts = None
    if last_ts is not None:
        next_start = last_ts + interval_delta
    else:
        if settings.start is None:
            raise ValueError("A2_START must be provided before the first cache build.")
        next_start = settings.start
    stop_at = settings.end
    window = timedelta(hours=settings.window_hours)
    resource_id = f"{settings.network}:metrics"
    total_appended = 0

    while True:
        if stop_at and next_start >= stop_at:
            LOGGER.info("Reached configured end timestamp; stopping backfill.")
            break
        window_end = next_start + window
        if stop_at:
            window_end = min(window_end, stop_at)
        LOGGER.info("Fetching window %s -> %s", next_start.isoformat(), window_end.isoformat())
        frames: List[pd.DataFrame] = []
        for facility_id in facilities_df["facility_id"]:
            tidy = fetch_timeseries(
                client,
                settings.network,
                facility_id,
                settings.metrics,
                next_start,
                window_end,
                interval=settings.interval,
            )
            if not tidy.empty:
                frames.append(tidy)
        if not frames:
            LOGGER.info("No rows returned for window; stopping backfill.")
            break
        raw_metrics = pd.concat(frames, ignore_index=True)
        enriched = attach_facility_meta(pivot_metrics(reconcile_units(raw_metrics)), facilities_df)
        changed, md5 = append_csv_cache(enriched, csv_path)
        total_appended += len(enriched)
        _update_manifest(
            settings.manifest_path,
            resource_id=resource_id,
            cache_path=csv_path,
            md5=md5,
            retrieved_at=datetime.utcnow(),
            unchanged=not changed,
        )
        next_start = window_end
        if client.remaining_budget == 0:
            LOGGER.warning("API request budget exhausted; stopping backfill.")
            break
    if total_appended == 0:
        LOGGER.info("Cache already up to date; no rows appended.")
    return csv_path


def build_cache(settings: Settings) -> Path:
    """Fetch facilities, extend the CSV cache, and update the manifest."""
    with OpenElectricityClient(
        settings.api_key,
        request_budget=settings.request_budget,
        alert_threshold=settings.request_budget_alert_threshold,
    ) as client:
        facilities_df = load_facilities(settings, client)
        csv_path = backfill_until_empty(settings, client, facilities_df)
    return csv_path


def _load_metrics_cache(csv_path: Path) -> pd.DataFrame:
    """Load the metrics CSV with parsed timestamps."""
    if not csv_path.exists():
        return pd.DataFrame(columns=["facility_id", "ts_event", *SERIES_COLUMN_MAP.values()])
    df = pd.read_csv(csv_path, parse_dates=["ts_event"])
    df = df.sort_values(["ts_event", "facility_id"]).reset_index(drop=True)
    return df


def stream_latest(settings: Settings, cache_path: Optional[Path] = None) -> None:
    """Publish new rows from the CSV cache via MQTT."""
    csv_path = cache_path or settings.metrics_path
    if not csv_path.exists():
        LOGGER.warning("CSV cache %s not found; run build mode first.", csv_path)
        return
    data = _load_metrics_cache(csv_path)
    if data.empty:
        LOGGER.info("CSV cache %s is empty; nothing to stream.", csv_path)
        return
    watermark = load_publish_offset(settings.publish_offsets_path)
    if watermark is not None:
        data = data[data["ts_event"] > watermark]
    if data.empty:
        LOGGER.info("No new rows after watermark %s; nothing to publish.", watermark)
        return
    facilities_df = pd.read_csv(settings.facilities_path) if settings.facilities_path.exists() else data
    facility_lookup = _facility_points_from_df(facilities_df)
    events_df = data[["facility_id", "ts_event", *SERIES_COLUMN_MAP.values()]].copy()
    LOGGER.info(
        "phase=stream cache=%s rows=%s facilities=%s watermark=%s",
        csv_path,
        len(events_df),
        len(facility_lookup),
        watermark.isoformat() if watermark else "none",
    )

    def _advance_watermark(event: MetricEvent) -> None:
        save_publish_offset(settings.publish_offsets_path, event.ts_event)

    publish_events(
        events_df,
        facility_lookup,
        host=settings.mqtt_host,
        port=settings.mqtt_port,
        keepalive=settings.mqtt_keepalive,
        delay_s=0.1,
        on_published=_advance_watermark,
    )


def loop(settings: Settings) -> None:
    """Continuously publish new rows, backfill the CSV cache, and sleep."""
    LOGGER.info("Starting loop mode with %.1fs delay between iterations.", settings.loop_delay)
    try:
        while True:
            cycle_start = time.perf_counter()
            try:
                stream_latest(settings)
                build_cache(settings)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Loop iteration failed: %s", exc)
            cycle_duration = time.perf_counter() - cycle_start
            sleep_for = max(0.0, settings.loop_delay - cycle_duration)
            LOGGER.info("phase=loop duration=%.2fs sleep=%.2fs", cycle_duration, sleep_for)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        LOGGER.info("Loop interrupted by user; shutting down.")


# ---------------------------------------------------------------------------
# CLI


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="COMP5339 A2 backend pipeline")
    parser.add_argument("--mode", choices=["build", "stream", "loop"], required=True)
    parser.add_argument("--start", help="Override start timestamp (ISO format).")
    parser.add_argument("--end", help="Override end timestamp (ISO format).")
    parser.add_argument("--network", help="Network identifier (default NEM).")
    parser.add_argument("--cache-dir", help="Directory for facilities/metrics caches.")
    parser.add_argument("--metrics", help="Comma-separated metrics (subset of power,emissions,price,demand).")
    parser.add_argument("--interval", help="Sampling interval, e.g. 5m.")
    parser.add_argument("--loop-delay", type=float, help="Seconds between loop iterations (>=60).")
    parser.add_argument("--window-hours", type=int, help="Backfill window size in hours (default 24).")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = _build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    settings = Settings.from_env()
    overrides: Dict[str, object] = {}
    if args.start:
        overrides["start"] = _parse_nullable_datetime(args.start)
    if args.end:
        overrides["end"] = _parse_nullable_datetime(args.end)
    if args.network:
        overrides["network"] = args.network
    if args.cache_dir:
        overrides["cache_dir"] = Path(args.cache_dir)
    if args.metrics:
        parts = tuple(part.strip() for part in args.metrics.split(",") if part.strip())
        invalid = [m for m in parts if m not in SERIES_COLUMN_MAP]
        if invalid:
            parser.error(f"Unsupported metrics: {invalid}")
        overrides["metrics"] = parts
    if args.interval:
        _parse_interval(args.interval)  # validate
        overrides["interval"] = args.interval
    if args.loop_delay is not None:
        overrides["loop_delay"] = max(float(args.loop_delay), 60.0)
    if args.window_hours is not None:
        overrides["window_hours"] = max(int(args.window_hours), 1)

    settings = settings.with_overrides(**overrides) if overrides else settings
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    LOGGER.debug("Resolved settings: %s", settings)
    ensure_data_dirs([settings.cache_dir])

    if args.mode == "build":
        build_cache(settings)
    elif args.mode == "stream":
        stream_latest(settings)
    elif args.mode == "loop":
        loop(settings)
    else:
        parser.error(f"Unsupported mode {args.mode}")


if __name__ == "__main__":
    main()
