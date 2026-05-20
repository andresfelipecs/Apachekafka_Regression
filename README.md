# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) · Universidad Autónoma de Occidente  
**Dataset:** World Happiness Report 2015–2019 (Kaggle: `unsdsn/world-happiness`)

---

## Project Description

A streaming ETL pipeline that:
1. Cleans and harmonizes World Happiness CSV files (2015–2019) into a unified schema.
2. Trains a regression model to predict happiness score.
3. Streams records via Apache Kafka.
4. Consumes events in real-time, runs ML inference, and stores results in PostgreSQL.
5. Exposes KPI queries for analytical dashboards.

---

## Architecture

```
Offline (Part A)
  CSV files (raw/) → EDA → Cleaning & Harmonization → Feature Engineering
                   → Train LinearRegression → Save model.pkl

Streaming (Part B + C)
  CSV files → Kafka Producer → Topic: happiness-predictions
                             → Kafka Consumer
                                  ├─ Store raw event (raw_happiness_events)
                                  ├─ Validate schema
                                  ├─ Load model.pkl → Predict
                                  └─ Store results (fact_predictions)
                                          ↓
                               PostgreSQL DB → KPI Queries → Dashboard
```

---

## Folder Structure

```
workshop-3/
├── data/
│   ├── raw/               # Original CSV files (2015–2019)
│   ├── processed/         # Unified dataset + visualizations
│   └── streaming/
├── notebooks/
│   ├── eda.ipynb          # Exploratory Data Analysis
│   └── model_training.ipynb
├── kafka/
│   ├── producer.py        # Kafka producer
│   └── consumer.py        # Kafka consumer with ML inference
├── models/
│   ├── model.pkl          # Serialized LinearRegression
│   └── feature_meta.pkl   # Feature list for consistent inference
├── sql/
│   ├── create_tables.sql  # PostgreSQL schema
│   └── kpis.sql           # Analytical KPI queries
├── dashboards/            # Dashboard screenshots
├── docker-compose.yml     # Kafka + Zookeeper + PostgreSQL
├── requirements.txt
└── README.md
```

---

## Data Cleaning Decisions

| Decision | Justification |
|---|---|
| Dropped Dystopia Residual, Standard Error, Confidence Intervals, Whisker columns | Year-specific, non-predictive for cross-year model |
| Renamed all columns to snake_case canonical names | Inconsistent naming across 2015–2017 (dot notation) and 2018–2019 |
| Filled missing `region` with `'Unknown'` | Region only present in 2015–2016; NaN would break groupby |
| Year-level median imputation for feature NaNs | Preserves distribution without introducing cross-year bias |
| Dropped rows where `happiness_score` is NaN | Target cannot be missing |

---

## Feature Engineering

| Feature | Formula | Justification |
|---|---|---|
| `social_wellbeing` | `family + health` | High correlation with happiness (r ≈ 0.73) |
| `positive_factors` | `gdp + freedom + generosity − corruption` | Composite positive indicator |

Core features used: `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`, `social_wellbeing`, `positive_factors`

Model: **Linear Regression** (focus is pipeline integration, not accuracy optimization).

---

## Kafka Pipeline

### Topic
`happiness-predictions`

### Event Schema (JSON)
```json
{
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2
}
```

### Producer
Streams each row from `data/processed/happiness_unified.csv` one by one with configurable delay.

### Consumer
1. Receives event
2. Stores raw payload in `raw_happiness_events` (with status: VALID / INVALID_SCHEMA / INVALID_VALUES / PREDICTION_ERROR)
3. Validates schema (required fields, numeric types, no NaN/Inf)
4. Reconstructs engineered features in the correct order
5. Runs `model.predict()`
6. Inserts into `fact_predictions` linked to `dim_country` and `dim_date`

---

## Database Schema

```
raw_happiness_events (raw_event_id PK, received_at, raw_payload JSONB, processing_status)
dim_country         (country_id PK, country_name, region)
dim_date            (date_id PK, year)
dim_raw_event       (raw_event_id PK FK, country, year, gdp, family, health, freedom, generosity, corruption)
fact_predictions    (prediction_id PK, raw_event_id FK, country_id FK, date_id FK,
                     actual_score, predicted_score, prediction_error, prediction_timestamp)
```

---

## Dashboard KPIs

| # | KPI | Query file |
|---|---|---|
| 1 | Average prediction error | `kpis.sql` |
| 2 | Predictions by country | `kpis.sql` |
| 3 | Predicted vs actual score | `kpis.sql` |
| 4 | Prediction trends over time | `kpis.sql` |

Suggested tools: Power BI, Looker Studio, Tableau (connect to PostgreSQL).

---

## Execution Instructions

### Prerequisites
- Docker Desktop running
- Python 3.12+
- Install dependencies: `pip install -r requirements.txt`

### 1. Start infrastructure
```bash
docker-compose up -d
```
Wait ~15 seconds for Kafka and PostgreSQL to be ready.

### 2. Run EDA notebook
```bash
jupyter notebook notebooks/eda.ipynb
```
Or execute directly:
```bash
python -m nbconvert --to notebook --execute notebooks/eda.ipynb --output notebooks/eda.ipynb
```

### 3. Train model
```bash
python -m nbconvert --to notebook --execute notebooks/model_training.ipynb --output notebooks/model_training.ipynb
```
This generates `models/model.pkl`.

### 4. Start consumer (in a separate terminal)
```bash
python kafka/consumer.py
```

### 5. Start producer (in another terminal)
```bash
# Stream all records with 0.5s delay
python kafka/producer.py

# Or limit to 20 events with 1s delay
python kafka/producer.py --limit 20 --delay 1.0
```

### 6. Query KPIs
```bash
docker exec -it postgres psql -U etl_user -d happiness_db -f /docker-entrypoint-initdb.d/kpis.sql
```
Or connect any BI tool to `localhost:5432 / happiness_db / etl_user / etl_pass`.

### 7. Stop infrastructure
```bash
docker-compose down
```
