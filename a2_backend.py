"""Assignment 2 backend pipeline entry point.

This module orchestrates the data engineering workflow for COMP5339 A2.
It provides a CLI with build/stream/loop modes and exposes pure helpers
for individual steps so the frontend can import and reuse shared logic.

Usage
-----
Run `python a2_backend.py --mode build` to fetch data into the cache,
`--mode stream` to publish the most recent cache over MQTT, or
`--mode loop` to continuously rebuild and stream on a timer. Environment
variables such as `OE_API_KEY`, `A2_START`, and `A2_END` supply defaults,
while CLI flags override individual settings. Use `--facility-cache-only`
to reuse cached facility metadata without calling the facilities API, and
tune `A2_BUDGET_ALERT_THRESHOLD` to receive early warnings before the
request budget is exhausted.

Logic overview
--------------
1. Resolve ``Settings`` from env/CLI, prepare cache/tmp folders, and guard API budget.
2. Discover facilities for the requested network and optionally filter by codes.
3. Pull raw metric time series from OpenElectricity and tidy them into one DataFrame.
4. Harmonise units, pivot to a wide schema, enrich with facility metadata, and write parquet + manifest.
5. Stream mode reuses the cache to emit MQTT ``MetricEvent`` payloads (dry-run skips publishes).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from paho.mqtt import client as mqtt

LOGGER = logging.getLogger("a2.backend")
BASE_URL = "https://api.openelectricity.org.au"
FACILITIES_ENDPOINT = "/v1/facilities/"
DATA_ENDPOINT_TEMPLATE = "/v4/data/facilities/{network}"
REQUEST_TIMEOUT = 60
REQUEST_MIN_INTERVAL = 0.25  # seconds between calls
SERIES_COLUMN_MAP = {
    "power": "power_mw",
    "emissions": "co2_t",
    "price": "price",
    "demand": "demand",
}
MANIFEST_FILENAME = "manifest.csv"


# ---------------------------------------------------------------------------
# HTTP client & rate limiting


class RequestBudget:
    """Track the remaining API request allowance."""

    def __init__(self, limit: int, *, warn_threshold: float = 0.1) -> None:
        """Initialise the budget with a daily limit, zero usage, and optional low-budget warning."""
        self.limit = limit
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
    """Thin HTTP client with pacing, retry, and request budget tracking."""

    def __init__(self, api_key: str, *, request_budget: int, alert_threshold: float = 0.1) -> None:
        """Create a session bound to the OpenElectricity API with a request budget."""
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})
        self._budget = RequestBudget(request_budget, warn_threshold=alert_threshold)
        self._last_request_ts = 0.0

    def _pace(self) -> None:
        """Sleep just enough to respect the minimum interval between requests."""
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < REQUEST_MIN_INTERVAL:
            time.sleep(REQUEST_MIN_INTERVAL - elapsed)

    def _request(self, method: str, path: str, *, params: Optional[Sequence[Tuple[str, Any]]] = None) -> requests.Response:
        """Perform an HTTP request with retry/backoff handling and budget tracking."""
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
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                detail = ""
                try:
                    detail = response.json().get("error")  # type: ignore[assignment]
                except Exception:
                    detail = response.text[:200]
                LOGGER.error(
                    "HTTP error %s for %s %s params=%s detail=%s",
                    response.status_code,
                    method,
                    url,
                    params,
                    detail,
                )
                raise
        response.raise_for_status()
        return response

    def get_json(self, path: str, *, params: Optional[Sequence[Tuple[str, Any]]] = None) -> Dict[str, Any]:
        """Issue a GET request and return the parsed JSON payload."""
        response = self._request("GET", path, params=params)
        return response.json()

    def close(self) -> None:
        """Close the underlying requests session."""
        if self._budget.used > 0:
            LOGGER.info(
                "API usage summary: used=%s remaining=%s limit=%s",
                self._budget.used,
                self._budget.remaining,
                self._budget.limit,
            )
        self._session.close()

    def __enter__(self) -> "OpenElectricityClient":
        """Support context-manager usage."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        """Ensure the HTTP session is closed when leaving a context."""
        self.close()

# ---------------------------------------------------------------------------
# Settings & configuration


def _parse_nullable_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp string if provided."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO timestamp '{value}'") from exc
    if parsed.tzinfo is not None:
        # API expects timezone naive timestamps in network-local time.
        parsed = parsed.replace(tzinfo=None)
    return parsed


@dataclass(frozen=True)
class Settings:
    """Runtime configuration resolved from env vars and CLI flags."""

    api_key: str
    network: str = "NEM"
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    cache_dir: Path = field(default_factory=lambda: Path("data/cache"))
    tmp_dir: Path = field(default_factory=lambda: Path("data/tmp"))
    request_budget: int = 450
    request_budget_alert_threshold: float = 0.1
    metrics: Tuple[str, ...] = ("power", "emissions")
    facility_codes: Optional[Tuple[str, ...]] = None
    facility_cache_only: bool = False
    cache_only: bool = False
    interval: str = "5m"
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_keepalive: int = 60
    loop_delay: float = 60.0
    dry_run: bool = False
    log_level: str = "INFO"
    facility_cache_path: Path = field(default_factory=lambda: Path("data/cache/facilities_catalog.parquet"))
    facility_cache_ttl: int = 86400

    @staticmethod
    def from_env() -> "Settings":
        """Construct settings using environment variables."""
        env = os.environ
        api_key = env.get("OE_API_KEY") or env.get("A2_API_KEY")
        if not api_key:
            raise RuntimeError("Missing OE_API_KEY env var for OpenElectricity access.")

        start = _parse_nullable_datetime(env.get("A2_START"))
        end = _parse_nullable_datetime(env.get("A2_END"))

        cache_dir = Path(env.get("A2_CACHE_DIR", "data/cache"))
        tmp_dir = Path(env.get("A2_TMP_DIR", "data/tmp"))

        request_budget = int(env.get("A2_REQUEST_BUDGET", "450"))
        request_budget_alert_threshold = float(env.get("A2_BUDGET_ALERT_THRESHOLD", "0.1"))
        metrics_env = env.get("A2_METRICS")
        metrics = tuple(m.strip() for m in metrics_env.split(",")) if metrics_env else ("power", "emissions")
        facilities_env = env.get("A2_FACILITY_CODES")
        facility_codes = tuple(code.strip() for code in facilities_env.split(",")) if facilities_env else None
        facility_cache_only = env.get("A2_FACILITY_CACHE_ONLY", "0") in {"1", "true", "True"}
        cache_only = env.get("A2_CACHE_ONLY", "0") in {"1", "true", "True"}
        if cache_only:
            facility_cache_only = True
        interval = env.get("A2_INTERVAL", "5m")
        loop_delay = float(env.get("A2_LOOP_DELAY", "60"))
        mqtt_host = env.get("MQTT_HOST", "localhost")
        mqtt_port = int(env.get("MQTT_PORT", "1883"))
        mqtt_keepalive = int(env.get("MQTT_KEEPALIVE", "60"))
        dry_run = env.get("A2_DRY_RUN", "0") in {"1", "true", "True"}
        log_level = env.get("A2_LOG_LEVEL", "INFO")
        facility_cache_path = Path(env.get("A2_FACILITY_CACHE_PATH", str(cache_dir / "facilities_catalog.parquet")))
        facility_cache_ttl = int(env.get("A2_FACILITY_CACHE_TTL", "86400"))

        return Settings(
            api_key=api_key,
            network=env.get("A2_NETWORK", "NEM"),
            start=start,
            end=end,
            cache_dir=cache_dir,
            tmp_dir=tmp_dir,
            request_budget=request_budget,
            request_budget_alert_threshold=request_budget_alert_threshold,
            metrics=metrics,
            facility_codes=facility_codes,
            facility_cache_only=facility_cache_only,
            cache_only=cache_only,
            interval=interval,
            mqtt_host=mqtt_host,
            mqtt_port=mqtt_port,
            mqtt_keepalive=mqtt_keepalive,
            loop_delay=loop_delay,
            dry_run=dry_run,
            log_level=log_level,
            facility_cache_path=facility_cache_path,
            facility_cache_ttl=facility_cache_ttl,
        )

    def with_overrides(self, **kwargs: object) -> "Settings":
        """Return a copy with updated attributes."""
        return Settings(**{**self.__dict__, **kwargs})


# ---------------------------------------------------------------------------
# Domain models & message contracts


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
        """Serialize the event to a JSON payload."""
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
# Data retrieval & integration helpers


def _format_timestamp(value: Optional[datetime]) -> Optional[str]:
    """Serialise datetimes as API-friendly strings while preserving None."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def discover_facilities(
    client: OpenElectricityClient,
    network: str,
    *,
    cache_path: Optional[Path] = None,
    cache_ttl: Optional[int] = None,
    require_cache: bool = False,
) -> pd.DataFrame:
    """Fetch facility metadata for the given network with optional caching or cache-only operation."""
    cache_file: Optional[Path] = cache_path if cache_path is not None else None
    if cache_file is not None:
        try:
            if cache_file.exists():
                cached = pd.read_parquet(cache_file)
                if not cached.empty:
                    age_seconds = time.time() - cache_file.stat().st_mtime
                    ttl = cache_ttl if cache_ttl is not None else 0
                    if require_cache:
                        LOGGER.debug("Loaded facility metadata from cache %s (cache only mode)", cache_file)
                        return cached
                    if ttl <= 0 or age_seconds <= ttl:
                        LOGGER.debug("Loaded facility metadata from cache %s (age=%.0fs)", cache_file, age_seconds)
                        return cached
                elif require_cache:
                    raise RuntimeError(f"Facility cache at {cache_file} is empty; cannot continue with cache-only mode.")
            elif require_cache:
                raise RuntimeError(f"Facility cache not found at {cache_file}; run a build without cache-only mode first.")
        except Exception as exc:  # pragma: no cover - cache read is best-effort
            if require_cache:
                raise RuntimeError(f"Failed to read facility cache {cache_file}: {exc}") from exc
            LOGGER.warning("Failed to load facility cache %s: %s", cache_file, exc)
    elif require_cache:
        raise RuntimeError("Facility cache path not provided; cannot run in cache-only mode.")

    params: List[Tuple[str, Any]] = [("network_id", network)]
    payload = client.get_json(FACILITIES_ENDPOINT, params=params)
    facilities = payload.get("data", [])
    rows: List[Dict[str, Any]] = []
    for facility in facilities:
        code = facility.get("code")
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
        raise RuntimeError("Facility metadata contains missing facility_id values.")
    missing_mask = df["lat"].isna() | df["lon"].isna()
    if missing_mask.any():
        missing = df.loc[missing_mask, "facility_id"].tolist()
        LOGGER.warning("Dropping facilities without coordinates: %s", missing)
        df = df[~missing_mask]
    bounds_mask = (df["lat"].between(-45, -10)) & (df["lon"].between(110, 160))
    if (~bounds_mask).any():
        outliers = df.loc[~bounds_mask, "facility_id"].tolist()
        LOGGER.warning("Dropping facilities outside coordinate bounds: %s", outliers)
        df = df[bounds_mask]
    df = df.sort_values("facility_id").reset_index(drop=True)
    if cache_file is not None:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache_file, index=False)
            LOGGER.debug("Wrote facility metadata cache to %s", cache_file)
        except Exception as exc:  # pragma: no cover - cache write is best-effort
            LOGGER.warning("Failed to write facility cache %s: %s", cache_file, exc)
    return df


def fetch_timeseries(
    client: OpenElectricityClient,
    network: str,
    facility_id: str,
    metrics: Sequence[str],
    start: datetime,
    end: datetime,
    interval: str = "5m",
) -> pd.DataFrame:
    """Retrieve per-unit metric series for a facility."""
    params: List[Tuple[str, Any]] = []
    for metric in metrics:
        params.append(("metrics", metric))
    params.append(("facility_code", facility_id))
    params.append(("date_start", _format_timestamp(start)))
    params.append(("date_end", _format_timestamp(end)))
    params.append(("interval", interval))

    try:
        payload = client.get_json(DATA_ENDPOINT_TEMPLATE.format(network=network), params=params)
    except requests.HTTPError as exc:  # type: ignore[attr-defined]
        status = getattr(exc.response, "status_code", None)
        if status == 416:
            LOGGER.info("phase=fetch facility=%s status=no_data", facility_id)
            return pd.DataFrame(columns=["facility_id", "series", "unit_code", "ts", "value"])
        raise
    rows: List[Dict[str, Any]] = []
    if not payload.get("success", True):
        LOGGER.warning(
            "phase=fetch facility=%s status=error message=%s",
            facility_id,
            payload.get("error"),
        )
        return pd.DataFrame(columns=["facility_id", "series", "unit_code", "ts", "value"])

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
    if (grouped["metric_value"] < 0).any():
        negatives = grouped[grouped["metric_value"] < 0][["facility_id", "series", "ts_event"]].head()
        LOGGER.warning("Negative metric values found after unit reconciliation: \n%s", negatives)
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
    if merged["name"].isna().any():
        missing = merged[merged["name"].isna()]["facility_id"].unique().tolist()
        raise RuntimeError(f"Missing metadata for facilities: {missing}")
    return merged


# ---------------------------------------------------------------------------
# Cache IO & manifest management


def _compute_md5(path: Path) -> str:
    """Return the hexadecimal MD5 digest of a file's contents."""
    digest = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_parquet(path: Path) -> pd.DataFrame:
    """Load a parquet dataset if it exists, else return empty DataFrame."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def write_parquet(
    df: pd.DataFrame,
    path: Path,
    *,
    manifest_path: Path,
    resource_id: str,
    retrieved_at: Optional[datetime] = None,
) -> Path:
    """Write parquet data to disk and update the manifest if content changed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp_path, index=False)
    new_md5 = _compute_md5(tmp_path)

    existing_md5 = _compute_md5(path) if path.exists() else None
    if existing_md5 == new_md5:
        LOGGER.info("No changes detected for %s; keeping existing cache.", path)
        tmp_path.unlink(missing_ok=True)
        _update_manifest(manifest_path, resource_id, path, new_md5, retrieved_at, unchanged=True)
        return path

    tmp_path.replace(path)
    _update_manifest(manifest_path, resource_id, path, new_md5, retrieved_at, unchanged=False)
    LOGGER.info("Wrote %s (%s bytes)", path, path.stat().st_size)
    return path


def _update_manifest(
    manifest_path: Path,
    resource_id: str,
    cache_path: Path,
    md5: str,
    retrieved_at: Optional[datetime],
    *,
    unchanged: bool,
) -> None:
    """Record the cache write in `manifest.csv`, replacing any prior entry for the resource."""
    retrieved_at = retrieved_at or datetime.utcnow()
    entry = {
        "resource_id": resource_id,
        "cache_path": str(cache_path),
        "retrieved_at": retrieved_at.isoformat(),
        "bytes": cache_path.stat().st_size if cache_path.exists() else 0,
        "md5": md5,
        "status": "unchanged" if unchanged else "updated",
    }
    manifest_df = pd.read_csv(manifest_path) if manifest_path.exists() else pd.DataFrame(columns=list(entry.keys()))
    manifest_df = manifest_df[manifest_df["resource_id"] != resource_id]
    manifest_df = pd.concat([manifest_df, pd.DataFrame([entry])], ignore_index=True)
    manifest_df.to_csv(manifest_path, index=False)


# ---------------------------------------------------------------------------
# MQTT publishing helpers


def _maybe_float(value: Any) -> Optional[float]:
    """Convert pandas scalar to optional float."""
    if value is None:
        return None
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _make_mqtt_client() -> mqtt.Client:
    """Create an MQTT client using the newest callback API."""
    callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api_version is not None:
        try:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION2)
        except AttributeError:
            return mqtt.Client(callback_api_version=callback_api_version.VERSION1)
    return mqtt.Client()


def _facility_points_from_df(df: pd.DataFrame) -> Dict[str, FacilityPoint]:
    """Build a lookup of facility metadata keyed by facility id."""
    required = {"facility_id", "name", "fuel", "state", "lat", "lon"}
    if not required.issubset(df.columns):
        missing = required.difference(df.columns)
        raise ValueError(f"Missing required metadata columns: {missing}")
    deduped = df[sorted(required)].drop_duplicates("facility_id")
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


def find_latest_cache(cache_dir: Path) -> Path:
    """Return the most recently modified parquet cache file."""
    parquet_files = sorted(cache_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet cache files found in {cache_dir}")
    return parquet_files[0]


def _get_row_value(row: Any, name: str) -> Any:
    """Support both attribute-style and dict-like access for row iterables."""
    return getattr(row, name) if hasattr(row, name) else row[name]


def _event_from_row(row: Any, *, ts_ingest: Optional[datetime] = None) -> MetricEvent:
    """Transform a metrics row into a ``MetricEvent`` ready for MQTT publish."""
    ts_raw = _get_row_value(row, "ts_event")
    ts_event = pd.to_datetime(ts_raw).to_pydatetime()
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
) -> None:
    """Publish metric events to MQTT in timestamp order."""
    if metrics_df.empty:
        LOGGER.warning("No events to publish.")
        return

    delay_s = max(delay_s, 0.1)
    client = _make_mqtt_client()
    client.enable_logger(LOGGER)
    try:
        client.connect(host, port, keepalive)
    except OSError as exc:
        LOGGER.error(
            "MQTT connection failed host=%s port=%s keepalive=%s error=%s",
            host,
            port,
            keepalive,
            exc,
        )
        return
    try:
        ordered = metrics_df.sort_values(["ts_event", "facility_id"]).itertuples(index=False)
        for row in ordered:
            facility_id = str(getattr(row, "facility_id"))
            facility = facility_lookup.get(facility_id)
            if not facility:
                LOGGER.warning("Skipping facility %s without metadata.", facility_id)
                continue
            event = _event_from_row(row)
            topic = topic_for(facility)
            payload = event.to_json()
            info = client.publish(topic, payload, qos=0, retain=False)
            info.wait_for_publish()
            LOGGER.info(
                "phase=publish facility=%s topic=%s ts_event=%s",
                facility_id,
                topic,
                event.ts_event.isoformat(),
            )
            time.sleep(delay_s)
    finally:
        client.disconnect()

# ---------------------------------------------------------------------------
# File system helpers


def ensure_data_dirs(paths: Iterable[Path]) -> None:
    """Create required directories, if missing."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# CLI orchestration


def _build_arg_parser() -> argparse.ArgumentParser:
    """Create the CLI parser shared by unit tests and the ``main`` entry point."""
    parser = argparse.ArgumentParser(description="COMP5339 Assignment 2 backend pipeline")
    parser.add_argument("--mode", choices={"build", "stream", "loop"}, default="build")
    parser.add_argument("--start", help="ISO date start override (defaults to $A2_START)")
    parser.add_argument("--end", help="ISO date end override (defaults to $A2_END)")
    parser.add_argument("--network", help="Network code (default NEM)")
    parser.add_argument("--cache-dir", help="Path for cache outputs")
    parser.add_argument("--tmp-dir", help="Path for temporary files")
    parser.add_argument("--metrics", help="Comma separated metric list (default power,emissions)")
    parser.add_argument("--facility", action="append", help="Facility code to include (repeatable)")
    parser.add_argument(
        "--facility-cache-only",
        action="store_true",
        help="Load facility metadata only from the cached parquet; do not call the facilities API.",
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Reuse an existing metrics cache and skip all API requests.",
    )
    parser.add_argument("--interval", help="Metric interval (default 5m)")
    parser.add_argument("--loop-delay", type=float, help="Seconds to wait between loop cycles (default 60)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate without writes or publishes")
    parser.add_argument("--log-level", default=None, help="Logging level override")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    """
    Entry point for the A2 backend pipeline CLI.

    Parses CLI flags (or supplied ``argv`` for testability), merges them with
    environment defaults, and dispatches to the requested mode:
    ``build`` writes a fresh cache, ``stream`` publishes the latest cache, and
    ``loop`` alternates between both operations until interrupted.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    settings = Settings.from_env()
    overrides = {}
    if args.start:
        overrides["start"] = _parse_nullable_datetime(args.start)
    if args.end:
        overrides["end"] = _parse_nullable_datetime(args.end)
    if args.network:
        overrides["network"] = args.network
    if args.cache_dir:
        overrides["cache_dir"] = Path(args.cache_dir)
    if args.tmp_dir:
        overrides["tmp_dir"] = Path(args.tmp_dir)
    if args.metrics:
        overrides["metrics"] = tuple(part.strip() for part in args.metrics.split(","))
    if args.facility:
        overrides["facility_codes"] = tuple(args.facility)
    if args.facility_cache_only:
        overrides["facility_cache_only"] = True
    if args.cache_only:
        overrides["cache_only"] = True
        overrides.setdefault("facility_cache_only", True)
    if args.interval:
        overrides["interval"] = args.interval
    if args.loop_delay is not None:
        overrides["loop_delay"] = float(args.loop_delay)
    if args.dry_run:
        overrides["dry_run"] = True
    if args.log_level:
        overrides["log_level"] = args.log_level

    settings = settings.with_overrides(**overrides) if overrides else settings
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    LOGGER.debug("Resolved settings: %s", settings)

    ensure_data_dirs([settings.cache_dir, settings.tmp_dir])

    if args.mode == "build":
        build_cache(settings)
    elif args.mode == "stream":
        stream_latest(settings)
    elif args.mode == "loop":
        loop(settings)
    else:
        parser.error(f"Unsupported mode {args.mode}")


# ---------------------------------------------------------------------------
# Pipeline orchestration stubs (implemented in subsequent steps)


def build_cache(settings: Settings) -> Path:
    """Fetch, clean, and persist the latest dataset returning the cache path.

    Steps:
        1. Optionally reuse an existing cache when ``cache_only`` is set.
        2. Validate the requested window and derive an output filename.
        3. Discover facilities (optionally narrowed to ``facility_codes``) using the shared HTTP client.
        4. Iterate over each facility, downloading time-series metrics and joining them into a single tidy frame.
        5. Reconcile units, pivot to a wide schema, and enrich with facility metadata so parquet rows are dashboard-ready.
        6. Persist the parquet file and update ``manifest.csv`` unless ``dry_run`` is enabled.

    Notes:
        * ``cache_only`` expects a parquet produced by a prior successful build (with metric columns such as
          ``power_mw`` and ``ts_event``); it will surface an error later in streaming if the cached file is incomplete.
    """
    if settings.cache_only:
        if settings.start is not None and settings.end is not None:
            target_name = f"{settings.network.lower()}_{settings.start.strftime('%Y%m%d%H%M')}_{settings.end.strftime('%Y%m%d%H%M')}.parquet"
            candidate = settings.cache_dir / target_name
            if candidate.exists():
                LOGGER.info("Cache-only mode enabled; reusing existing cache %s", candidate)
                return candidate
            LOGGER.warning("Cache-only mode requested but target cache %s not found; falling back to latest cache.", candidate)
        try:
            latest = find_latest_cache(settings.cache_dir)
        except FileNotFoundError as exc:
            raise RuntimeError(
                "Cache-only mode requested but no cache files are available. Run a full build at least once first."
            ) from exc
        LOGGER.info("Cache-only mode enabled; reusing latest cache %s", latest)
        return latest

    if settings.start is None or settings.end is None:
        raise ValueError("Both start and end timestamps must be provided via env vars or CLI.")
    if settings.end <= settings.start:
        raise ValueError("End timestamp must be after start timestamp.")

    window_label = f"{settings.start.strftime('%Y%m%d%H%M')}_{settings.end.strftime('%Y%m%d%H%M')}"
    cache_path = settings.cache_dir / f"{settings.network.lower()}_{window_label}.parquet"
    manifest_path = settings.cache_dir / MANIFEST_FILENAME
    retrieved_at = datetime.utcnow()

    LOGGER.info(
        "phase=build_cache mode=%s start=%s end=%s metrics=%s facilities=%s",
        "dry_run" if settings.dry_run else "write",
        settings.start.isoformat(),
        settings.end.isoformat(),
        ",".join(settings.metrics),
        "all" if settings.facility_codes is None else ",".join(settings.facility_codes),
    )

    facilities_df: pd.DataFrame
    with OpenElectricityClient(
        settings.api_key,
        request_budget=settings.request_budget,
        alert_threshold=settings.request_budget_alert_threshold,
    ) as client:
        facilities_df = discover_facilities(
            client,
            settings.network,
            cache_path=settings.facility_cache_path,
            cache_ttl=settings.facility_cache_ttl,
            require_cache=settings.facility_cache_only,
        )
        if settings.facility_codes:
            missing = sorted(set(settings.facility_codes) - set(facilities_df["facility_id"]))
            if missing:
                raise RuntimeError(f"Requested facilities not found: {missing}")
            facilities_df = facilities_df[facilities_df["facility_id"].isin(settings.facility_codes)].reset_index(drop=True)
        LOGGER.info("phase=facilities rows=%s", len(facilities_df))

        metric_frames: List[pd.DataFrame] = []
        for facility_id in facilities_df["facility_id"]:
            t0 = time.perf_counter()
            tidy = fetch_timeseries(
                client,
                settings.network,
                facility_id,
                settings.metrics,
                settings.start,
                settings.end,
                interval=settings.interval,
            )
            duration = time.perf_counter() - t0
            LOGGER.info("phase=fetch facility=%s rows=%s duration=%.2fs", facility_id, len(tidy), duration)
            if not tidy.empty:
                metric_frames.append(tidy)

    if not metric_frames:
        LOGGER.warning("No metric data retrieved for the requested window; cache remains unchanged.")
        return cache_path

    raw_metrics = pd.concat(metric_frames, ignore_index=True)
    reconciled = reconcile_units(raw_metrics)
    wide_metrics = pivot_metrics(reconciled)
    enriched = attach_facility_meta(wide_metrics, facilities_df)
    LOGGER.info(
        "phase=transform rows_raw=%s rows_reconciled=%s rows_wide=%s",
        len(raw_metrics),
        len(reconciled),
        len(enriched),
    )

    if settings.dry_run:
        LOGGER.info("Dry run enabled; skipping cache write.")
        return cache_path

    resource_id = f"{settings.network}:{window_label}"
    write_parquet(enriched, cache_path, manifest_path=manifest_path, resource_id=resource_id, retrieved_at=retrieved_at)
    return cache_path


def stream_latest(settings: Settings, cache_path: Optional[Path] = None) -> None:
    """Publish MetricEvents from the cache to MQTT.

    Requires the parquet to include ``facility_id``, ``ts_event`` and the wide metric columns
    (``power_mw``, ``co2_t``, ``price``, ``demand``). These are only present in caches created
    by a full (non ``cache_only``/``dry_run``) build.
    """
    try:
        target_path = cache_path or find_latest_cache(settings.cache_dir)
    except FileNotFoundError as exc:
        LOGGER.error("No cache available to stream: %s", exc)
        return

    data = read_parquet(target_path)
    if data.empty:
        LOGGER.warning("Cache %s is empty; nothing to stream.", target_path)
        return

    event_columns = ["facility_id", "ts_event", *SERIES_COLUMN_MAP.values()]
    missing_columns = [col for col in event_columns if col not in data.columns]
    if missing_columns:
        raise RuntimeError(f"Cache missing required columns: {missing_columns}")

    facility_lookup = _facility_points_from_df(data)
    events_df = data[event_columns].copy()
    LOGGER.info(
        "phase=stream cache=%s events=%s facilities=%s",
        target_path,
        len(events_df),
        len(facility_lookup),
    )

    if settings.dry_run:
        preview = events_df.head(5).itertuples(index=False)
        for row in preview:
            event = _event_from_row(row)
            LOGGER.info("dry_run_event facility=%s payload=%s", event.facility_id, event.to_json())
        LOGGER.info("Dry run enabled; MQTT publish skipped.")
        return

    publish_events(
        events_df,
        facility_lookup,
        host=settings.mqtt_host,
        port=settings.mqtt_port,
        keepalive=settings.mqtt_keepalive,
        delay_s=0.1,
    )


def loop(settings: Settings) -> None:
    """Continuously refresh cache and stream latest data."""
    LOGGER.info("Starting loop mode with %.1fs delay between iterations.", settings.loop_delay)
    try:
        while True:
            cycle_start = time.perf_counter()
            try:
                cache_path = build_cache(settings)
                stream_latest(settings, cache_path=cache_path)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Loop iteration failed: %s", exc)
            cycle_duration = time.perf_counter() - cycle_start
            sleep_for = max(0.0, settings.loop_delay - cycle_duration)
            LOGGER.info("phase=loop duration=%.2fs sleep=%.2fs", cycle_duration, sleep_for)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        LOGGER.info("Loop interrupted by user; shutting down.")


if __name__ == "__main__":
    main()
