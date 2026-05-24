"""
Client for the OpenSky Network REST API.

Polls /states/all within a geographic bounding box and returns parsed
flight state records. Handles rate limits and network errors gracefully.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional
import logging
import time

import requests

logger = logging.getLogger(__name__)

# OpenSky positional indices for /states/all rows.
# Reference: https://openskynetwork.github.io/opensky-api/rest.html
ICAO24 = 0
CALLSIGN = 1
ORIGIN_COUNTRY = 2
LONGITUDE = 5
LATITUDE = 6
BARO_ALTITUDE = 7
ON_GROUND = 8
VELOCITY = 9
TRUE_TRACK = 10
VERTICAL_RATE = 11
GEO_ALTITUDE = 13


@dataclass
class FlightState:
    """One aircraft observation. Will map to fact_telemetry_event later."""
    icao24: str
    callsign: Optional[str]
    origin_country: str
    event_timestamp: str           # ISO8601 UTC
    longitude: float
    latitude: float
    baro_altitude_m: Optional[float]
    geo_altitude_m: Optional[float]
    velocity_ms: Optional[float]
    true_track_deg: Optional[float]
    vertical_rate_ms: Optional[float]
    on_ground: bool

    def to_dict(self) -> dict:
        return asdict(self)


# Continental US bounding box. Tight enough to keep payloads under
# a few hundred KB; wide enough to see a few hundred aircraft.
US_BBOX = {
    "lamin": 24.0,    # south
    "lomin": -125.0,  # west
    "lamax": 49.0,    # north
    "lomax": -66.0,   # east
}


class OpenSkyClient:
    """Polls OpenSky /states/all for one bbox and returns parsed states."""

    BASE_URL = "https://opensky-network.org/api/states/all"

    def __init__(self, bbox: dict = US_BBOX, timeout_sec: int = 15):
        self.bbox = bbox
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "aura-edge/0.1 (portfolio)"})

    def fetch_states(self) -> list[FlightState]:
        """Hit the API and return clean FlightState records.

        Returns [] on rate-limit or transient errors — caller should keep
        polling. We log and continue rather than crash, because a polling
        loop that crashes on the first 429 is useless in real ops.
        """
        try:
            response = self.session.get(
                self.BASE_URL, params=self.bbox, timeout=self.timeout_sec
            )
        except requests.RequestException as exc:
            logger.warning("OpenSky request failed: %s", exc)
            return []

        if response.status_code == 429:
            logger.warning("OpenSky rate-limited (429); backing off 5s")
            time.sleep(5)
            return []
        if response.status_code != 200:
            logger.warning("OpenSky returned HTTP %d", response.status_code)
            return []

        payload = response.json()
        if not payload or not payload.get("states"):
            return []

        snapshot_time = payload["time"]
        return [
            parsed for parsed in (
                self._parse_row(row, snapshot_time) for row in payload["states"]
            )
            if parsed is not None
        ]

    def _parse_row(self, row: list, snapshot_time: int) -> Optional[FlightState]:
        """Convert one OpenSky positional row to a FlightState, or None if unusable."""
        icao24 = row[ICAO24]
        lat = row[LATITUDE]
        lon = row[LONGITUDE]
        if not icao24 or lat is None or lon is None:
            return None  # We can't do anything with an aircraft we can't locate.

        callsign = row[CALLSIGN]
        if callsign:
            callsign = callsign.strip() or None  # OpenSky pads callsigns with spaces.

        return FlightState(
            icao24=icao24,
            callsign=callsign,
            origin_country=row[ORIGIN_COUNTRY] or "Unknown",
            event_timestamp=datetime.fromtimestamp(
                snapshot_time, tz=timezone.utc
            ).isoformat(),
            longitude=lon,
            latitude=lat,
            baro_altitude_m=row[BARO_ALTITUDE],
            geo_altitude_m=row[GEO_ALTITUDE],
            velocity_ms=row[VELOCITY],
            true_track_deg=row[TRUE_TRACK],
            vertical_rate_ms=row[VERTICAL_RATE],
            on_ground=bool(row[ON_GROUND]),
        )