# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) · Universidad Autónoma de Occidente  
**Dataset:** World Happiness Report 2015–2019 (Kaggle: `unsdsn/world-happiness`)

---

## What this project does

This project implements a **streaming ETL pipeline** that:

1. Cleans and harmonizes 5 years of World Happiness CSV data into a single unified schema (**Part A**)
2. Trains a Linear Regression model to predict happiness scores and serializes it as `model.pkl` (**Part A**)
3. Streams records one by one through Apache Kafka — simulating a real-time event feed (**Part B**)
4. Consumes each Kafka event, validates it, runs ML inference in real time, and stores results in PostgreSQL (**Part B**)
5. Exposes 4 analytical KPIs through a FastAPI server connected to a live HTML dashboard (**Part C**)

### Why Kafka instead of classic batch ETL?

| Classic batch ETL | Streaming ETL (this project) |
|---|---|
| Process all records at once on a schedule | Process each record the moment it arrives |
| OK when a delay of minutes/hours is acceptable | Required when you need to react in real time |
| Model **training** — needs all data together | Model **inference** — one row at a time is enough |

Kafka acts as the transport pipe between the producer (data source) and the consumer (ML inference engine). The producer and consumer are fully decoupled — if the consumer restarts, Kafka holds the messages until it catches up.

---

## Architecture

```
PART A — Offline / Batch
  CSV files (data/raw/)
    → EDA notebook           (notebooks/eda.ipynb)
    → Cleaning & harmonize   → data/processed/happiness_unified.csv
    → Feature engineering
    → Train LinearRegression → models/model.pkl + models/feature_meta.pkl

PART B — Streaming
  happiness_unified.csv
    → Kafka Producer         (kafka/producer.py)
    → Topic: happiness-predictions
    → Kafka Consumer         (kafka/consumer.py)
         ├─ Store raw event  → raw_happiness_events
         ├─ Validate schema
         ├─ Reconstruct features
         ├─ model.predict()
         └─ Store results    → fact_predictions (linked to dim_country, dim_date)

PART C — Analytics
  PostgreSQL
    → FastAPI (api.py)       → /api/kpi1  /api/kpi2  /api/kpi3  /api/kpi4
    → dashboard.html         (live charts, auto-refreshed from DB)
```

---

## Folder Structure

```
workshop-3/
├── data/
│   ├── raw/                    # Original CSV files (2015–2019) — do not modify
│   │   ├── 2015.csv
│   │   ├── 2016.csv
│   │   ├── 2017.csv
│   │   ├── 2018.csv
│   │   └── 2019.csv
│   └── processed/
│       ├── happiness_unified.csv       # Output of Part A cleaning
│       ├── correlation_heatmap.png
│       ├── feature_correlation.png
│       ├── model_evaluation.png
│       └── outliers_boxplot.png
├── notebooks/
│   ├── eda.ipynb               # Part A — Step 1: Exploratory Data Analysis
│   └── model_training.ipynb    # Part A — Steps 2-4: Cleaning, features, model
├── kafka/
│   ├── producer.py             # Part B — Step 5: Kafka producer
│   └── consumer.py             # Part B — Step 6: Kafka consumer + ML inference
├── models/
│   ├── model.pkl               # Serialized LinearRegression (output of training)
│   └── feature_meta.pkl        # Feature list — ensures inference uses same order as training
├── sql/
│   ├── create_tables.sql       # Part C — Step 7: PostgreSQL schema
│   └── kpis.sql                # Part C — Step 9: Analytical KPI queries
├── api.py                      # Part C — FastAPI server (serves dashboard + KPI endpoints)
├── dashboard.html              # Part C — Interactive HTML dashboard
├── docker-compose.yml          # Kafka + Zookeeper + PostgreSQL
├── requirements.txt
└── README.md
```

---

## Prerequisites

- **Docker Desktop** — must be running before anything else
- **Python 3.10+**
- A terminal (three separate terminal windows/tabs needed for Step 4)

---

## Step-by-step: Run from scratch

### Step 0 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

### Step 1 — Start infrastructure (Docker)

```bash
docker-compose up -d
```

This starts three containers:
- **Zookeeper** on port 2181 (required by Kafka internally)
- **Kafka** on port 9092
- **PostgreSQL** on port 5432 — database `happiness_db`, user `etl_user`, password `etl_pass`

PostgreSQL automatically runs `sql/create_tables.sql` on first boot, creating all tables.

Wait ~15 seconds for Kafka to be ready before running the producer/consumer.

Verify everything is up:
```bash
docker-compose ps
```

---

### Step 2 — Part A: Run EDA notebook

```bash
jupyter notebook notebooks/eda.ipynb
```

Or run headlessly:
```bash
python -m nbconvert --to notebook --execute notebooks/eda.ipynb --output notebooks/eda.ipynb
```

This produces visualizations in `data/processed/` and validates the data quality of all 5 CSV files.

---

### Step 3 — Part A: Train the model

```bash
jupyter notebook notebooks/model_training.ipynb
```

Or headlessly:
```bash
python -m nbconvert --to notebook --execute notebooks/model_training.ipynb --output notebooks/model_training.ipynb
```

This produces:
- `models/model.pkl` — the serialized LinearRegression model
- `models/feature_meta.pkl` — the ordered feature list used at inference time
- `data/processed/happiness_unified.csv` — the cleaned, harmonized dataset

> **Note:** `model.pkl` is already committed to this repository. You only need to re-run training if you change the feature engineering or model type.

---

### Step 4 — Part B: Run the streaming pipeline

You need **two separate terminal windows** for this step.

**Terminal 1 — Start the Kafka consumer** (must start first):
```bash
cd kafka
python consumer.py
```

The consumer connects to Kafka and PostgreSQL, loads `model.pkl`, and waits for events.
You will see: `Listening on topic 'happiness-predictions'...`

**Terminal 2 — Start the Kafka producer**:
```bash
cd kafka
python producer.py
```

By default this streams all 782 records with a 0.5 second delay between each.

Optional flags:
```bash
python producer.py --limit 20        # Only stream 20 events
python producer.py --delay 1.0       # 1 second between events
python producer.py --limit 50 --delay 0.2
```

Watch Terminal 1 to see predictions being generated in real time:
```
[OK] raw_id=1 | Switzerland 2015 | actual=7.587 predicted=7.108 error=0.479
[OK] raw_id=2 | Iceland 2015     | actual=7.561 predicted=6.832 error=0.729
...
```

---

### Step 5 — Part C: Start the dashboard API

In a **third terminal**:
```bash
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Then open: **http://localhost:8000**

The dashboard automatically fetches live data from PostgreSQL via the API endpoints. You will see a **🟢 Live — PostgreSQL** indicator in the top-right corner.

If the API is not running, the dashboard still works — it falls back to embedded snapshot data and shows **🟡 Offline snapshot**.

---

### Step 6 — Stop everything

```bash
docker-compose down
```

To also delete all stored data (wipe PostgreSQL volume):
```bash
docker-compose down -v
```

---

## API Endpoints

Once `uvicorn api:app` is running, the following endpoints are available:

| Endpoint | Description |
|---|---|
| `GET /` | Serves `dashboard.html` |
| `GET /api/kpi1` | Overall error stats (MAE, RMSE, total predictions, processing status) |
| `GET /api/kpi2` | Avg actual vs predicted score per country |
| `GET /api/kpi3` | Per-event scatter data (actual vs predicted) |
| `GET /api/kpi4` | Year-over-year prediction trends |
| `GET /api/recent` | Last 50 raw Kafka events |
| `GET /health` | DB connectivity check |
| `GET /docs` | Auto-generated Swagger UI |

---

## Database Schema

```
raw_happiness_events
  raw_event_id PK | received_at | raw_payload JSONB | processing_status
  → Stores every Kafka message exactly as received, before any processing
  → Status: VALID | INVALID_SCHEMA | INVALID_VALUES | PREDICTION_ERROR

dim_country
  country_id PK | country_name | region

dim_date
  date_id PK | year

dim_raw_event
  raw_event_id PK FK → raw_happiness_events
  country | year | gdp | family | health | freedom | generosity | corruption

fact_predictions
  prediction_id PK | raw_event_id FK | country_id FK | date_id FK
  actual_score | predicted_score | prediction_error | prediction_timestamp
```

**Why store raw events first?** The consumer inserts into `raw_happiness_events` *before* any validation or prediction. This enables full traceability — even invalid records are preserved with their error status, so you can audit, debug, or reprocess them later.

---

## Data Cleaning Decisions

| Decision | Justification |
|---|---|
| Dropped Dystopia Residual, Standard Error, Confidence Intervals, Whisker columns | Year-specific, non-predictive for a cross-year model |
| Renamed all columns to `snake_case` canonical names | Column naming was inconsistent across years (dot notation in 2015–2017, plain names in 2018–2019) |
| Filled missing `region` with `'Unknown'` | Region only present in 2015–2016; NaN would break `groupby` aggregations |
| Year-level median imputation for feature NaNs | Preserves distribution without introducing cross-year bias |
| Dropped rows where `happiness_score` is NaN | Target column cannot be missing for supervised learning |

---

## Feature Engineering

| Feature | Formula | Justification |
|---|---|---|
| `social_wellbeing` | `family + health` | High combined correlation with happiness score (r ≈ 0.73) |
| `positive_factors` | `gdp + freedom + generosity − corruption` | Composite indicator of positive environmental conditions |

**Core features used for training and inference:**
`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`, `social_wellbeing`, `positive_factors`

The `feature_meta.pkl` file stores this exact ordered list. The consumer loads it at startup to guarantee that inference always uses the same feature order as training — a common source of silent bugs in ML pipelines.

---

## Model

- **Algorithm:** Linear Regression (from scikit-learn)
- **Split:** 70% training / 30% testing
- **Metrics (test set):** MAE ≈ 0.43, RMSE ≈ 0.55, R² ≈ 0.76

> The focus of this workshop is **pipeline integration**, not model accuracy. Linear Regression is intentionally simple so the architecture is the main deliverable.

---

## Dashboard KPIs

| KPI | Description | Chart type |
|---|---|---|
| KPI 1 | Average prediction error, RMSE, R², total predictions | Stat cards |
| KPI 2 | Avg actual vs predicted score per country (top 20) | Grouped bar chart |
| KPI 3 | Per-country actual vs predicted scatter with perfect-fit line | Scatter plot |
| KPI 4 | Year-over-year prediction trends and error evolution | Line + bar charts |

**Bonus panels:** Event processing status breakdown, regional happiness overview, searchable country table.

---

## Evaluation Criteria (from workshop rubric)

| Criteria | Weight |
|---|---|
| Data Integration & Cleaning | 1.0 |
| Feature Engineering | 0.5 |
| ML Pipeline | 0.5 |
| Kafka Producer | 0.5 |
| Kafka Consumer | 0.5 |
| Event Validation | 0.5 |
| Database Design & Loading | 0.5 |
| Dashboard & KPIs | 0.5 |
| Documentation & Reproducibility | 0.5 |

Project: **70%** · Presentation: **30%**

---

## Troubleshooting

**`NoBrokersAvailable` error in producer/consumer**
→ Kafka is not ready yet. Wait 15–20 seconds after `docker-compose up -d` and retry.

**`psycopg2.OperationalError: could not connect to server`**
→ PostgreSQL container is not running. Check `docker-compose ps` and restart if needed.

**Consumer exits immediately**
→ Make sure `models/model.pkl` exists. Run the training notebook first (Step 3).

**Dashboard shows 🟡 Offline snapshot**
→ The FastAPI server is not running. Start it with `uvicorn api:app --port 8000`.

**Tables not created in PostgreSQL**
→ The `create_tables.sql` script runs automatically only on first boot. If you started Docker with `-v` previously (wiped the volume), run:
```bash
docker exec -i postgres psql -U etl_user -d happiness_db < sql/create_tables.sql
```
