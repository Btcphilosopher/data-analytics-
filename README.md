# AUREUM ANALYTICS

> **Autonomous Unified Runtime for Enterprise Modelling & Analytics**

A cloud-native, enterprise-grade data analytics and artificial intelligence platform engineered with industrial precision, hypermodern clarity, and Anglo-futuristic design philosophy.

---

## Architecture Overview

AUREUM is structured as a strict layered system. Each layer has a defined responsibility, a clean interface, and no cross-layer leakage.

```
Data Sources
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  INGESTION LAYER                                    │
│  REST Connector · File Connector · DB Connector     │
│  Stream Handler · Batch Loader · Schema Validation  │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  PREPROCESSING LAYER                                │
│  DataCleaner · Transformer · FeatureEngineer        │
│  Normalisation · Rolling Windows · Lag Features     │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  ML LAYER                                           │
│  XGBoost Regression · XGBoost Classification       │
│  Isolation Forest · Z-Score Detector               │
│  XGBoost Forecaster · Model Registry               │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  PIPELINE ENGINE                                    │
│  Composable async steps · YAML/JSON config          │
│  Run reports · Error strategies                     │
└──────────┬──────────────────────┬───────────────────┘
           │                      │
           ▼                      ▼
┌──────────────────┐   ┌──────────────────────────────┐
│  API LAYER       │   │  STREAMING LAYER             │
│  FastAPI         │   │  Event Bus · Pub/Sub         │
│  JWT Auth        │   │  Redis / In-Memory           │
│  Role-based ACL  │   │  AureumEvent envelope        │
└──────────────────┘   └──────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────┐
│  DASHBOARD                                          │
│  Plotly Dash · Anglo-futurist dark theme            │
│  Real-time charts · KPIs · Anomaly log             │
│  2-second live refresh                              │
└─────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
aureum_analytics/
├── config/                   # Settings (Pydantic), structured logging
├── ingestion/
│   ├── connectors/           # REST, File, Database connectors
│   ├── stream_handler.py     # Async buffer + flush engine
│   └── batch_loader.py       # Batch validation + load manifests
├── preprocessing/
│   ├── cleaning.py           # Dedup, null handling, outlier removal
│   ├── transformation.py     # Scaling, encoding, resampling
│   └── feature_engineering.py # Rolling, lag, temporal, technical indicators
├── ml/
│   ├── models/
│   │   ├── base_model.py     # Abstract contract: fit/predict/save/load
│   │   ├── regression.py     # XGBoost + Ridge regression
│   │   ├── classification.py # XGBoost + Random Forest classifier
│   │   ├── anomaly.py        # Isolation Forest, Z-Score, IQR detectors
│   │   └── forecasting.py    # XGBoost multi-step forecaster + CI
│   ├── training.py           # Training orchestrator + cross-validation
│   └── inference.py          # Production inference engine + caching
├── pipelines/
│   ├── pipeline_engine.py    # Async composable pipeline runtime
│   ├── config_loader.py      # YAML/JSON config → Pipeline builder
│   └── configs/              # Pipeline YAML definitions
├── api/
│   ├── app.py                # FastAPI application factory
│   ├── auth.py               # JWT authentication + RBAC
│   ├── schemas.py            # Pydantic request/response models
│   └── routes/routes.py      # All API route handlers
├── streaming/
│   └── event_bus.py          # In-memory + Redis pub/sub event bus
├── analytics/
│   └── aggregation.py        # Group-by, time-bucket, cohort, metrics
├── alerts/
│   └── alert_engine.py       # Anomaly alerts, threshold rules, priority
├── dashboard/
│   ├── app.py                # Dash application init
│   ├── layouts.py            # Full dashboard layout
│   ├── components.py         # Plotly chart factories
│   └── callbacks.py          # Live reactive callbacks
├── mock/
│   └── data_generator.py     # Synthetic financial, IoT, operational data
├── tests/                    # Pytest unit + integration tests
├── main.py                   # Platform entry point
├── requirements.txt
└── pyproject.toml
```

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ with TimescaleDB (optional for full stack)
- Redis 7+ (optional — falls back to in-memory)

### 1. Clone and install

```bash
git clone https://github.com/your-org/aureum-analytics.git
cd aureum-analytics

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r aureum_analytics/requirements.txt
```

### 2. Configure environment

```bash
cp aureum_analytics/.env.example aureum_analytics/.env
# Edit .env with your database URL, Redis URL, and secret key
```

### 3. Run modes

**API server only:**
```bash
python -m aureum_analytics.main api
# → http://localhost:8000/docs
```

**Dashboard only:**
```bash
python -m aureum_analytics.main dashboard
# → http://localhost:8050
```

**Both concurrently:**
```bash
python -m aureum_analytics.main all
```

**End-to-end demo pipeline:**
```bash
python -m aureum_analytics.main demo
```

---

## API Reference

All endpoints are prefixed with `/api/v1`.

| Method | Endpoint         | Auth | Description                         |
|--------|-----------------|------|-------------------------------------|
| POST   | `/auth/token`   | —    | Authenticate, receive JWT           |
| GET    | `/health`       | —    | System health check                 |
| GET    | `/data`         | ✓    | Retrieve data from named source     |
| POST   | `/ingest`       | ✓ W  | Ingest a batch of records           |
| GET    | `/analytics`    | ✓    | Time-bucketed aggregated analytics  |
| POST   | `/predict`      | ✓ P  | Single-record model inference       |
| GET    | `/anomalies`    | ✓    | Active anomaly alerts               |

**Demo credentials:**

| Username        | Password         | Role     | Permissions               |
|----------------|-----------------|----------|---------------------------|
| `aureum_admin`  | `sovereign2024` | admin    | read, write, predict, admin |
| `analyst_01`    | `analyst_pass`  | analyst  | read, predict             |
| `viewer_01`     | `viewer_pass`   | viewer   | read                      |

---

## Pipeline System

Pipelines are defined in YAML and loaded via `ConfigLoader`:

```yaml
pipeline:
  name: financial_preprocessing
  version: "1.0.0"
  steps:
    - name: clean
      type: clean
      params:
        null_strategy: ffill
        outlier_columns: [price, volume]

    - name: rolling_features
      type: rolling_mean
      params:
        column: price
        windows: [7, 14, 30]

    - name: lag_features
      type: lag_features
      params:
        column: price
        lags: [1, 2, 3, 7]
```

**Available step types:** `clean`, `normalise`, `rolling_mean`, `lag_features`, `temporal_features`, `resample`

**Execute a pipeline:**
```python
from aureum_analytics.pipelines.config_loader import ConfigLoader

pipeline = ConfigLoader.from_file("pipelines/configs/financial_pipeline.yaml")
result_df, report = await pipeline.run(input_df)
print(report.summary())
```

---

## ML Models

All models inherit from `AureumBaseModel` and share a consistent interface:

```python
from aureum_analytics.ml.models.regression import XGBoostRegressionModel
from aureum_analytics.ml.training import ModelTrainer

# Train
model = XGBoostRegressionModel(name="price_predictor")
trainer = ModelTrainer()
run = trainer.train(model, X_train, y_train, save=True)

# Infer
from aureum_analytics.ml.inference import InferenceEngine
engine = InferenceEngine()
engine.register_model("price_predictor", model)
result = engine.predict("price_predictor", features={"price_lag_1": 105.0})
print(result.to_dict())
```

**Anomaly detection:**
```python
from aureum_analytics.ml.models.anomaly import IsolationForestModel

detector = IsolationForestModel()
detector.fit(X_train)
result_df = detector.detect(X_new)
# result_df contains: is_anomaly, anomaly_score, anomaly_confidence
```

**Forecasting:**
```python
from aureum_analytics.ml.models.forecasting import XGBoostForecaster

forecaster = XGBoostForecaster(n_lags=14, horizon=30)
forecaster.fit(pd.DataFrame(), y=price_series)
forecast = forecaster.forecast(steps=30, freq="D")
df = forecast.to_dataframe()
# columns: timestamp, forecast, lower_bound, upper_bound
```

---

## Testing

```bash
# Full test suite
pytest aureum_analytics/tests/ -v

# Individual suites
pytest aureum_analytics/tests/test_preprocessing.py
pytest aureum_analytics/tests/test_ml_models.py
pytest aureum_analytics/tests/test_pipelines.py
pytest aureum_analytics/tests/test_api.py

# With coverage
pytest aureum_analytics/tests/ --cov=aureum_analytics --cov-report=term-missing
```

---

## Design Principles

AUREUM is built on three non-negotiable architectural constraints:

1. **Layered isolation** — ingestion, preprocessing, ML, API, and dashboard are independently testable and replaceable. No layer reaches into another's internals.

2. **Explicit data flow** — every transformation is logged, every pipeline step reports its input/output shape and duration. Nothing is implicit.

3. **Production discipline** — typed Python throughout, Pydantic for all boundaries, structured logs at every layer, no experimental patterns.

---

*AUREUM ANALYTICS v1.0.0 — Sovereign-grade intelligence infrastructure.*
