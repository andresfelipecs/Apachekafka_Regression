-- ============================================================
-- Workshop 3 — Streaming ETL with Apache Kafka
-- Database schema for happiness prediction pipeline
-- ============================================================

-- Dimension: country
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE,
    region       VARCHAR(100)
);

-- Dimension: date (year)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year    INTEGER NOT NULL UNIQUE
);

-- Raw table: stores every Kafka event exactly as received
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id       SERIAL PRIMARY KEY,
    received_at        TIMESTAMP DEFAULT NOW(),
    raw_payload        JSONB NOT NULL,
    processing_status  VARCHAR(30) NOT NULL DEFAULT 'VALID'
        CHECK (processing_status IN ('VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR'))
);

-- Dimension: raw event (for traceability)
CREATE TABLE IF NOT EXISTS dim_raw_event (
    raw_event_id INTEGER PRIMARY KEY REFERENCES raw_happiness_events(raw_event_id),
    country      VARCHAR(100),
    year         INTEGER,
    gdp          NUMERIC(10,4),
    family       NUMERIC(10,4),
    health       NUMERIC(10,4),
    freedom      NUMERIC(10,4),
    generosity   NUMERIC(10,4),
    corruption   NUMERIC(10,4)
);

-- Fact table: prediction results
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        SERIAL PRIMARY KEY,
    raw_event_id         INTEGER REFERENCES raw_happiness_events(raw_event_id),
    country_id           INTEGER REFERENCES dim_country(country_id),
    date_id              INTEGER REFERENCES dim_date(date_id),
    actual_score         NUMERIC(10,4),
    predicted_score      NUMERIC(10,4),
    prediction_error     NUMERIC(10,4),
    prediction_timestamp TIMESTAMP DEFAULT NOW()
);
