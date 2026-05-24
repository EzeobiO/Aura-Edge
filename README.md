# Aura-Edge

An aviation telemetry data platform that ingests real flight state data from the OpenSky Network API and synthetic engine telemetry, processes it through an edge-buffered Apache Beam pipeline with conflict-resolution sync, scores it with a scikit-learn failure-prediction model, and lands it in a dimensional warehouse.

**Status:** In active development. See [`docs/aura_edge_spec.md`](docs/aura_edge_spec.md) for the full architecture.


## Architecture

See [`docs/aura_edge_spec.md`](docs/aura_edge_spec.md) and [`docs/dimensional_model.md`](docs/dimensional_model.md).