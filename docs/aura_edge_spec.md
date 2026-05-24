# Aura-Edge: Decentralized Aviation Telemetry Pipeline

**Author:** Obie Ezeobi

---

## 1. What this project is

A data engineering project that ingests live aviation flight data, simulates an edge node that buffers telemetry under intermittent connectivity, runs an ML model to predict component failures, and lands clean data in a dimensional warehouse. A dashboard sits on top of the warehouse for human-facing monitoring.

---

## 2. Architecture

```
                       ┌─────────────────────┐
                       │  OpenSky Network    │  (public API)
                       │  /states/all        │
                       └──────────┬──────────┘
                                  │ HTTP poll every 10s
                                  ▼
              ┌──────────────────────────────────────────┐
              │  EDGE NODE (simulated, Python process)   │
              │  - polls OpenSky                         │
              │  - generates synthetic engine telemetry  │
              │    correlated with flight state          │
              │  - buffers in local SQLite               │
              │  - tags each event with vector clock     │
              │  - "offline mode" toggle                 │
              └──────────────────┬───────────────────────┘
                                 │ batch sync when "online"
                                 ▼
              ┌──────────────────────────────────────────┐
              │  SYNC LAYER (Python module)              │
              │  - resolves conflicts via LWW-Register   │
              │  - handles out-of-order / late events    │
              │  - emits to pipeline ingestion queue     │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │  BEAM PIPELINE (DirectRunner local;      │
              │                 Dataflow-compatible)     │
              │                                          │
              │  Source → Parse → Validate → Enrich      │
              │    → ML Score (sklearn) → Route          │
              │    → Sink (DuckDB warehouse)             │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │  DUCKDB WAREHOUSE (star schema)          │
              │  - fact_telemetry_event                  │
              │  - fact_maintenance_event                │
              │  - fact_flight                           │
              │  - dim_aircraft (SCD Type 2)             │
              │  - dim_airport, dim_airline, dim_part    │
              │  - dim_date, dim_time                    │
              └──────────────────┬───────────────────────┘
                                 │ SQL
                                 ▼
              ┌──────────────────────────────────────────┐
              │  FASTAPI READ LAYER  →  NEXT.JS UI       │
              │  (figma-designed, shadcn/ui)             │
              └──────────────────────────────────────────┘
```

---

## 3. Data sources

### Public

| Source | What it is used it for | URL |
|---|---|---|
| **OpenSky Network API** | Live flight state vectors — position, altitude, velocity, on-ground flag, ICAO24 aircraft ID. Free, no auth for basic use. | https://opensky-network.org/api/states/all |
| **BTS On-Time Performance** | Historical flight schedules + delay reasons. Free CSV download. | https://www.transtats.bts.gov |
| **OpenFlights airports.dat** | Reference data for `dim_airport` — ICAO/IATA codes, lat/lon, elevation. | https://openflights.org/data |
| **OpenFlights airlines.dat** | Reference data for `dim_airline`. | https://openflights.org/data |

### Synthetic

| Synthetic data | Why synthetic | How it's generated |
|---|---|---|
| **Engine telemetry** (temp, hydraulic pressure, vibration) | Airlines don't release this publicly. | Function of flight phase + injected anomalies. Cruise = stable; takeoff/climb = high temp; rare 1-2% anomalies (rising temp + pressure drop) seeded as "failure events" for the ML model to learn. |
| **Maintenance events** | Not joined to specific tail numbers publicly. | Correlated with synthetic anomalies; injected at realistic frequencies (scheduled 250 flight hours, reactive after high-anomaly windows). |
| **Failure labels** | Required for supervised ML. | Forward-looking 24h window: "did a failure event occur within 24h of this telemetry window?" |

---

## 4. Dimensional model

Star schema. Fact tables at the grain of one event; dimensions describe the actors (aircraft, airport, part) and time.

### Fact tables

**`fact_telemetry_event`** — grain: one telemetry reading per aircraft per minute
```sql
CREATE TABLE fact_telemetry_event (
    telemetry_event_id      BIGINT       PRIMARY KEY,  -- surrogate
    aircraft_key            INTEGER      NOT NULL,     -- FK dim_aircraft
    airport_key             INTEGER,                   -- FK dim_airport (nearest)
    date_key                INTEGER      NOT NULL,     -- FK dim_date (YYYYMMDD)
    time_key                INTEGER      NOT NULL,     -- FK dim_time (HHMMSS)
    event_timestamp         TIMESTAMP    NOT NULL,
    engine_temp_c           DOUBLE,
    hydraulic_pressure_psi  DOUBLE,
    vibration_amplitude     DOUBLE,
    altitude_ft             INTEGER,
    velocity_kt             DOUBLE,
    flight_phase            VARCHAR,                   -- taxi|takeoff|climb|cruise|descent|approach|landing
    source_node_id          VARCHAR,                   -- which edge node emitted this
    ingest_lag_seconds      DOUBLE,                    -- event_time - ingest_time
    failure_probability     DOUBLE,                    -- 0..1 from ML model
    alert_level             VARCHAR                    -- none|advisory|warning|critical
);
```

**`fact_maintenance_event`** — grain: one maintenance action per aircraft per part
```sql
CREATE TABLE fact_maintenance_event (
    maintenance_event_id   BIGINT     PRIMARY KEY,
    aircraft_key           INTEGER    NOT NULL,
    part_key               INTEGER    NOT NULL,
    date_key               INTEGER    NOT NULL,
    event_type             VARCHAR,                -- scheduled|predicted|reactive
    duration_minutes       INTEGER,
    cost_usd               DOUBLE,
    downtime_minutes       INTEGER,
    triggered_by_alert_id  BIGINT                  -- nullable FK to telemetry event
);
```

**`fact_flight`** — grain: one flight leg
```sql
CREATE TABLE fact_flight (
    flight_key             BIGINT     PRIMARY KEY,
    aircraft_key           INTEGER    NOT NULL,
    origin_airport_key     INTEGER    NOT NULL,
    destination_airport_key INTEGER   NOT NULL,
    airline_key            INTEGER    NOT NULL,
    scheduled_departure    TIMESTAMP,
    actual_departure       TIMESTAMP,
    scheduled_arrival      TIMESTAMP,
    actual_arrival         TIMESTAMP,
    delay_minutes          INTEGER,
    delay_reason_code      VARCHAR
);
```

### Dimensions

**`dim_aircraft`** — Type 2 SCD (track changes to fleet assignment, registration, etc.)
```sql
CREATE TABLE dim_aircraft (
    aircraft_key        INTEGER   PRIMARY KEY,  -- surrogate
    tail_number         VARCHAR,                -- natural key (e.g., N123DL)
    icao24              VARCHAR,                -- 6-char hex from ADS-B
    model               VARCHAR,
    manufacturer        VARCHAR,
    year_manufactured   INTEGER,
    engine_type         VARCHAR,
    airline_key         INTEGER,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN
);
```

**`dim_airport`**
```sql
CREATE TABLE dim_airport (
    airport_key   INTEGER   PRIMARY KEY,
    icao_code     VARCHAR,
    iata_code     VARCHAR,
    airport_name  VARCHAR,
    city          VARCHAR,
    country       VARCHAR,
    latitude      DOUBLE,
    longitude     DOUBLE,
    elevation_ft  INTEGER,
    timezone      VARCHAR
);
```

**`dim_airline`**, **`dim_part`**, **`dim_date`**, **`dim_time`** — standard shapes, full DDL in `warehouse/schema.sql`.

### Why these choices

- **Surrogate keys** on dims so I can rebuild source IDs without breaking facts, and so SCD Type 2 history works.
- **Date and time as separate dimensions** — classic Kimball. Lets me query "average engine temp by hour of day across all dates" cost effectively.
- **`alert_level` denormalized onto the fact** — alert_level is computed from `failure_probability`, but storing it avoids a band lookup at query time. A facts-store-the-grain pattern.
- **`source_node_id` on every fact row** — preserves edge provenance for the eventual-consistency story.

### BigQuery equivalent

Same shape, with `INT64` instead of `INTEGER`, `STRING` instead of `VARCHAR`, partition on `date_key` for facts, cluster on `aircraft_key`. DDL provided in `warehouse/schema_bigquery.sql`.

---

## 5. Tech stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python `requests` + APScheduler | Simple, runs anywhere, easy to demo |
| Edge buffer | SQLite | Built into Python; mimics a real on-device DB |
| Sync layer | Python module (custom) | Where the eventual-consistency logic lives |
| Pipeline | Apache Beam Python SDK (DirectRunner) | Dataflow-compatible; same code runs on GCP |
| ML | scikit-learn LogisticRegression | Small, fast, explainable; pickled model loaded as DoFn |
| Warehouse | DuckDB (local file) | Columnar, SQL, free; ports cleanly to BigQuery |
| API | FastAPI | Already in your stack |
| Frontend | Next.js + shadcn/ui + Tailwind | Already in your stack; generated from Figma |
| Orchestration | Plain Python `main.py` scripts for Day 1; Airflow only if time permits | Don't fight orchestrators on day 1 |

---

## 6. Phase Planning

### Plan 1

| Phase | What |
|---|---|
| 1 | Repo scaffold, Python env, dependencies, Git init |
| 2 | Ingestion: OpenSky poller + synthetic telemetry generator |
| 3 | Warehouse: schema DDL, dim load from OpenFlights |
| 4 | Beam pipeline v1: source → validate → load facts |

### Plan 2

| Phase | What |
|---|---|
| 5 | Edge sync layer: SQLite buffer + LWW-Register + offline toggle |
| 6 | ML scoring: train model, integrate into Beam as DoFn |
| 7 | FastAPI + Next.js dashboard (Figma-generated) |
| 8 | README, architecture diagram, sample queries, polish |

---

## 7. Deliverables checklist

- [ ] `README.md` with architecture diagram and ~6 sample queries
- [ ] `pipeline/` — Beam code, runnable with one command
- [ ] `warehouse/schema.sql` and `warehouse/schema_bigquery.sql`
- [ ] `edge/` — sync layer + offline mode toggle, with unit tests on conflict resolution
- [ ] `ml/` — training notebook + pickled model + feature documentation
- [ ] `api/` — FastAPI app reading from DuckDB
- [ ] `ui/` — Next.js dashboard
- [ ] `data/synthetic/` — generator scripts
- [ ] `docs/dimensional_model.md` — schema reasoning, grain statements

---