"""
api.py — Dashboard API for Workshop 3
Serves the dashboard HTML and exposes JSON endpoints that query PostgreSQL live.

Run:
    uvicorn api:app --reload --host 0.0.0.0 --port 8000

Then open:  http://localhost:8000
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ─────────────────────────────────────────────────────────────────────
# DB config — override with environment variables if needed
# ─────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "happiness_db"),
    "user":     os.getenv("DB_USER",     "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_pass"),
}


def get_conn():
    """Return a new psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


def query(sql: str, params=None) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Test DB connection on startup
    try:
        conn = get_conn()
        conn.close()
        print("✅  Connected to PostgreSQL")
    except Exception as e:
        print(f"⚠️  Could not connect to PostgreSQL: {e}")
        print("    Dashboard will load but charts will show an error.")
    yield


app = FastAPI(
    title="Workshop 3 — Streaming ETL Dashboard API",
    description="Live KPI endpoints backed by PostgreSQL prediction results",
    version="1.0.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────────
# Static files — images referenced in dashboard.html
# ─────────────────────────────────────────────────────────────────────
app.mount("/data", StaticFiles(directory="data"), name="data")

# ─────────────────────────────────────────────────────────────────────
# Serve dashboard HTML
# ─────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def serve_dashboard():
    return FileResponse("dashboard.html")


# ─────────────────────────────────────────────────────────────────────
# KPI 1 — Overall model performance
# ─────────────────────────────────────────────────────────────────────
@app.get("/api/kpi1", summary="KPI 1 — Overall prediction error stats")
def kpi1() -> dict[str, Any]:
    """
    Returns aggregate error metrics across all predictions stored in
    fact_predictions:  avg absolute error, avg signed error, RMSE,
    total prediction count, and a breakdown by processing status.
    """
    rows = query("""
        SELECT
            ROUND(AVG(ABS(prediction_error))::NUMERIC, 4)  AS avg_abs_error,
            ROUND(AVG(prediction_error)::NUMERIC, 4)        AS avg_signed_error,
            ROUND(
                SQRT(AVG(prediction_error * prediction_error))::NUMERIC, 4
            )                                               AS rmse,
            COUNT(*)                                        AS total_predictions
        FROM fact_predictions
    """)
    stats = rows[0] if rows else {}

    status_rows = query("""
        SELECT processing_status AS status, COUNT(*) AS count
        FROM raw_happiness_events
        GROUP BY processing_status
        ORDER BY count DESC
    """)

    return {
        "avg_abs_error":    float(stats.get("avg_abs_error") or 0),
        "avg_signed_error": float(stats.get("avg_signed_error") or 0),
        "rmse":             float(stats.get("rmse") or 0),
        "total_predictions": int(stats.get("total_predictions") or 0),
        "processing_status": [
            {"status": r["status"], "count": int(r["count"])}
            for r in status_rows
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# KPI 2 — Predictions by country
# ─────────────────────────────────────────────────────────────────────
@app.get("/api/kpi2", summary="KPI 2 — Avg predicted vs actual score per country")
def kpi2() -> list[dict]:
    """
    Returns one row per country with: total predictions, avg actual score,
    avg predicted score, and avg absolute error. Ordered by avg actual
    score descending so the happiest countries appear first.
    """
    rows = query("""
        SELECT
            c.country_name                                        AS country,
            COUNT(*)                                              AS total_predictions,
            ROUND(AVG(fp.actual_score)::NUMERIC, 4)              AS avg_actual,
            ROUND(AVG(fp.predicted_score)::NUMERIC, 4)           AS avg_predicted,
            ROUND(AVG(ABS(fp.prediction_error))::NUMERIC, 4)     AS avg_abs_error
        FROM fact_predictions fp
        JOIN dim_country c ON fp.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY avg_actual DESC
    """)
    return [
        {
            "country":           r["country"],
            "total_predictions": int(r["total_predictions"]),
            "avg_actual":        float(r["avg_actual"]),
            "avg_predicted":     float(r["avg_predicted"]),
            "avg_abs_error":     float(r["avg_abs_error"]),
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────
# KPI 3 — Predicted vs actual per event (scatter data)
# ─────────────────────────────────────────────────────────────────────
@app.get("/api/kpi3", summary="KPI 3 — Per-event actual vs predicted (scatter)")
def kpi3() -> list[dict]:
    """
    Returns every prediction event with country, year, actual score,
    predicted score, and error — used to render the scatter plot.
    """
    rows = query("""
        SELECT
            c.country_name      AS country,
            d.year,
            fp.actual_score     AS actual,
            fp.predicted_score  AS predicted,
            fp.prediction_error AS error,
            fp.prediction_timestamp
        FROM fact_predictions fp
        JOIN dim_country c ON fp.country_id = c.country_id
        JOIN dim_date    d ON fp.date_id    = d.date_id
        ORDER BY fp.prediction_timestamp
    """)
    return [
        {
            "country":   r["country"],
            "year":      int(r["year"]),
            "actual":    float(r["actual"]),
            "predicted": float(r["predicted"]),
            "error":     float(r["error"]),
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────
# KPI 4 — Prediction trends over time (by year)
# ─────────────────────────────────────────────────────────────────────
@app.get("/api/kpi4", summary="KPI 4 — Prediction trends by year")
def kpi4() -> list[dict]:
    """
    Returns one row per year with: total predictions, avg actual score,
    avg predicted score, and avg absolute error. Used for trend charts.
    """
    rows = query("""
        SELECT
            d.year,
            COUNT(*)                                          AS total_predictions,
            ROUND(AVG(fp.actual_score)::NUMERIC, 4)          AS avg_actual,
            ROUND(AVG(fp.predicted_score)::NUMERIC, 4)       AS avg_predicted,
            ROUND(AVG(ABS(fp.prediction_error))::NUMERIC, 4) AS avg_abs_error
        FROM fact_predictions fp
        JOIN dim_date d ON fp.date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year
    """)
    return [
        {
            "year":              int(r["year"]),
            "total_predictions": int(r["total_predictions"]),
            "avg_actual":        float(r["avg_actual"]),
            "avg_predicted":     float(r["avg_predicted"]),
            "avg_abs_error":     float(r["avg_abs_error"]),
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────
# Bonus — Recent events (last 50 raw events)
# ─────────────────────────────────────────────────────────────────────
@app.get("/api/recent", summary="Last 50 raw Kafka events")
def recent_events() -> list[dict]:
    """Returns the 50 most recent raw Kafka events for the live feed panel."""
    rows = query("""
        SELECT
            raw_event_id,
            received_at,
            processing_status,
            raw_payload->>'country' AS country,
            raw_payload->>'year'    AS year
        FROM raw_happiness_events
        ORDER BY received_at DESC
        LIMIT 50
    """)
    return [
        {
            "raw_event_id":      int(r["raw_event_id"]),
            "received_at":       r["received_at"].isoformat() if r["received_at"] else None,
            "processing_status": r["processing_status"],
            "country":           r["country"],
            "year":              r["year"],
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────
@app.get("/health", summary="Health check — DB connectivity")
def health() -> dict:
    try:
        rows = query("SELECT COUNT(*) AS n FROM fact_predictions")
        return {"status": "ok", "predictions_in_db": int(rows[0]["n"])}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unreachable: {e}")
