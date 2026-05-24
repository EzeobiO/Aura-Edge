"""
Generates synthetic engine telemetry correlated with real flight state.

For each aircraft observed, produces (engine_temp, hydraulic_pressure,
vibration) values that depend on the inferred flight phase. Maintains
per-aircraft state across calls so values evolve smoothly. Occasionally
injects "anomaly trajectories" — slowly rising temp + dropping pressure —
which serve as positive labels for the ML failure model.
"""

from dataclasses import dataclass, asdict
from typing import Optional
import random

from ingestion.opensky_client import FlightState


# Per-phase baseline distributions: (mean, std) for each metric.
# Numbers are plausible for a commercial turbofan but not aviation-realistic;
# this is synthetic data with the right SHAPE, not the right values.
PHASE_BASELINES = {
    "taxi":     {"temp": (100, 10),  "pressure": (2900, 50),  "vibration": (1.0, 0.3)},
    "takeoff":  {"temp": (880, 30),  "pressure": (3100, 50),  "vibration": (5.5, 1.0)},
    "climb":    {"temp": (780, 25),  "pressure": (3050, 40),  "vibration": (4.0, 0.7)},
    "cruise":   {"temp": (650, 20),  "pressure": (3000, 30),  "vibration": (2.2, 0.5)},
    "descent":  {"temp": (550, 25),  "pressure": (3000, 40),  "vibration": (3.0, 0.6)},
    "approach": {"temp": (600, 30),  "pressure": (3050, 50),  "vibration": (3.5, 0.7)},
    "landing":  {"temp": (650, 35),  "pressure": (3150, 60),  "vibration": (6.5, 1.2)},
}

# Unit conversions
MS_TO_FPM = 196.85   # vertical rate: m/s → ft/min
M_TO_FT = 3.281      # altitude: meters → feet
MS_TO_KT = 1.944     # velocity: m/s → knots

# Anomaly injection parameters
ANOMALY_START_PROB = 0.002              # ~1 in 500 readings becomes anomalous
ANOMALY_DURATION_SEC = (300, 1800)      # anomaly lasts 5–30 minutes
ANOMALY_TEMP_DRIFT = (0.05, 0.25)       # temp rises this many °C per second
ANOMALY_PRESSURE_DRIFT = (-0.5, -0.1)   # pressure drops this many psi per second


@dataclass
class TelemetryReading:
    """One synthetic telemetry reading. Will map to fact_telemetry_event."""
    icao24: str
    event_timestamp: str
    flight_phase: str
    altitude_ft: Optional[int]
    velocity_kt: Optional[float]
    engine_temp_c: float
    hydraulic_pressure_psi: float
    vibration_amplitude: float
    is_anomaly: bool   # ground-truth label for supervised ML training

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class _AircraftState:
    """Internal state for one aircraft, carried between readings."""
    last_temp: float = 650.0
    last_pressure: float = 3000.0
    last_vibration: float = 2.0
    anomaly_active: bool = False
    anomaly_temp_rate: float = 0.0
    anomaly_pressure_rate: float = 0.0
    anomaly_remaining_sec: float = 0.0


def infer_flight_phase(state: FlightState) -> str:
    """Classify flight phase from real OpenSky fields.

    Rules are a simplified version of how flight ops actually categorizes
    phases — usable as a first approximation for synthetic generation.
    """
    if state.on_ground:
        return "taxi"

    altitude_ft = (state.baro_altitude_m or 0) * M_TO_FT
    vrate_fpm = (state.vertical_rate_ms or 0) * MS_TO_FPM

    if vrate_fpm > 500 and altitude_ft < 10000:
        return "takeoff"
    if vrate_fpm > 500:
        return "climb"
    if vrate_fpm < -500 and altitude_ft < 1500:
        return "landing"
    if vrate_fpm < -500 and altitude_ft < 10000:
        return "approach"
    if vrate_fpm < -500:
        return "descent"
    return "cruise"


class TelemetryGenerator:
    """Stateful telemetry generator. One instance per process.

    Not thread-safe — but we're single-threaded in the orchestrator,
    so this is fine. If we ever parallelize, we'd add a lock around
    the _aircraft dict.
    """

    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)  # seeded RNG → reproducible runs
        self._aircraft: dict[str, _AircraftState] = {}

    def generate(self, state: FlightState) -> TelemetryReading:
        phase = infer_flight_phase(state)
        baseline = PHASE_BASELINES[phase]
        aircraft = self._aircraft.setdefault(state.icao24, _AircraftState())

        # Maybe start an anomaly on this healthy aircraft
        if (not aircraft.anomaly_active
                and self._rng.random() < ANOMALY_START_PROB):
            self._start_anomaly(aircraft)

        # Draw target values from this phase's distribution
        target_temp = self._rng.gauss(*baseline["temp"])
        target_pressure = self._rng.gauss(*baseline["pressure"])
        target_vibration = max(0.1, self._rng.gauss(*baseline["vibration"]))

        # Exponential moving average — smooths values over time so each
        # aircraft's telemetry doesn't jump randomly between readings.
        # alpha=0.3 means 30% new target, 70% inherited from previous reading.
        alpha = 0.3
        temp = alpha * target_temp + (1 - alpha) * aircraft.last_temp
        pressure = alpha * target_pressure + (1 - alpha) * aircraft.last_pressure
        vibration = alpha * target_vibration + (1 - alpha) * aircraft.last_vibration

        # Apply anomaly drift if this aircraft is in a failure trajectory.
        # We assume ~10 seconds between readings (matches POLL_INTERVAL_SEC).
        if aircraft.anomaly_active:
            temp += aircraft.anomaly_temp_rate * 10
            pressure += aircraft.anomaly_pressure_rate * 10
            aircraft.anomaly_remaining_sec -= 10
            if aircraft.anomaly_remaining_sec <= 0:
                aircraft.anomaly_active = False

        # Persist for next reading
        aircraft.last_temp = temp
        aircraft.last_pressure = pressure
        aircraft.last_vibration = vibration

        altitude_ft = (
            int(state.baro_altitude_m * M_TO_FT)
            if state.baro_altitude_m is not None else None
        )
        velocity_kt = (
            round(state.velocity_ms * MS_TO_KT, 2)
            if state.velocity_ms is not None else None
        )

        return TelemetryReading(
            icao24=state.icao24,
            event_timestamp=state.event_timestamp,
            flight_phase=phase,
            altitude_ft=altitude_ft,
            velocity_kt=velocity_kt,
            engine_temp_c=round(temp, 2),
            hydraulic_pressure_psi=round(pressure, 2),
            vibration_amplitude=round(vibration, 3),
            is_anomaly=aircraft.anomaly_active,
        )

    def _start_anomaly(self, aircraft: _AircraftState) -> None:
        aircraft.anomaly_active = True
        aircraft.anomaly_temp_rate = self._rng.uniform(*ANOMALY_TEMP_DRIFT)
        aircraft.anomaly_pressure_rate = self._rng.uniform(*ANOMALY_PRESSURE_DRIFT)
        aircraft.anomaly_remaining_sec = self._rng.uniform(*ANOMALY_DURATION_SEC)