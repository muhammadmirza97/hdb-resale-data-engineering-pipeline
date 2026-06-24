"""
Step 9 — Alerts
===============
Scans clean_hdb_resale and the predictions table for unusual patterns:

  1. Towns with price_per_sqm far above/below the overall median
     (configurable thresholds in config.yaml)
  2. Large model prediction errors (pct_error > 30%)
  3. Recent transaction volume drops vs the previous month

Saves alerts to:
  - reports/alerts.csv
  - DuckDB table: alerts
"""

import logging

import pandas as pd

from src.config_loader import load_config
from src.paths import ALERTS_REPORT_PATH
from src.storage import get_connection, table_exists

logger = logging.getLogger(__name__)


def check_alerts() -> list[dict]:
    """
    Run all alert checks and return a list of alert dicts.
    """
    config = load_config()
    clean_table = config["tables"]["clean_table"]
    pred_table = config["tables"]["predictions_table"]
    alerts_table = config["tables"]["alerts_table"]
    high_ratio = config["alerts"]["price_per_sqm_high_ratio"]
    low_ratio  = config["alerts"]["price_per_sqm_low_ratio"]

    conn = get_connection()
    alerts: list[dict] = []
    now = pd.Timestamp.now().isoformat()

    # ── Alert 1: Town price_per_sqm anomalies ─────────────────────────────────
    town_df = conn.execute(f"""
        WITH overall AS (
            SELECT MEDIAN(price_per_sqm) AS overall_median
            FROM {clean_table}
        ),
        town_stats AS (
            SELECT town,
                COUNT(*)                      AS txn_count,
                ROUND(MEDIAN(price_per_sqm),2) AS median_psm
            FROM {clean_table}
            GROUP BY town
        )
        SELECT t.town, t.txn_count, t.median_psm,
               o.overall_median,
               ROUND(t.median_psm / o.overall_median, 2) AS ratio
        FROM town_stats t, overall o
        WHERE t.median_psm > o.overall_median * {high_ratio}
           OR t.median_psm < o.overall_median * {low_ratio}
        ORDER BY ratio DESC
    """).df()

    for _, row in town_df.iterrows():
        direction = "above" if row["ratio"] > 1 else "below"
        severity  = "HIGH" if abs(row["ratio"] - 1) > 0.6 else "MEDIUM"
        alerts.append({
            "alert_type": "TOWN_PRICE_ANOMALY",
            "town":       row["town"],
            "message":    (
                f"{row['town']}: median $/sqm = ${row['median_psm']:,.0f} "
                f"({row['ratio']:.1f}x {direction} overall median ${row['overall_median']:,.0f})"
            ),
            "severity":   severity,
            "created_at": now,
        })

    # ── Alert 2: High prediction error rows ──────────────────────────────────
    if table_exists(conn, pred_table):
        high_err_count = conn.execute(f"""
            SELECT COUNT(*) FROM {pred_table}
            WHERE pct_error > 30
        """).fetchone()[0]

        if high_err_count > 0:
            pct_high = conn.execute(f"""
                SELECT ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {pred_table}), 1)
                FROM {pred_table} WHERE pct_error > 30
            """).fetchone()[0]
            alerts.append({
                "alert_type": "HIGH_PREDICTION_ERROR",
                "town":       "ALL",
                "message":    f"{high_err_count:,} predictions have >30% error ({pct_high}% of total)",
                "severity":   "MEDIUM" if pct_high < 10 else "HIGH",
                "created_at": now,
            })

    # ── Alert 3: Recent month transaction volume drop ─────────────────────────
    volume_df = conn.execute(f"""
        SELECT sale_year, sale_month, COUNT(*) AS txn_count
        FROM {clean_table}
        GROUP BY sale_year, sale_month
        ORDER BY sale_year DESC, sale_month DESC
        LIMIT 3
    """).df()

    if len(volume_df) >= 2:
        latest_count = volume_df.iloc[0]["txn_count"]
        prev_count   = volume_df.iloc[1]["txn_count"]
        if prev_count > 0:
            change_pct = (latest_count - prev_count) / prev_count * 100
            if abs(change_pct) > 30:
                direction = "drop" if change_pct < 0 else "spike"
                alerts.append({
                    "alert_type": "VOLUME_CHANGE",
                    "town":       "ALL",
                    "message":    (
                        f"Latest month transaction count: {latest_count:,} "
                        f"({direction} of {abs(change_pct):.1f}% vs previous month {prev_count:,})"
                    ),
                    "severity":   "MEDIUM",
                    "created_at": now,
                })

    # ── Save alerts ───────────────────────────────────────────────────────────
    ALERTS_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    alerts_df = pd.DataFrame(alerts) if alerts else pd.DataFrame(
        columns=["alert_type", "town", "message", "severity", "created_at"]
    )
    alerts_df.to_csv(ALERTS_REPORT_PATH, index=False)
    conn.execute(f"CREATE OR REPLACE TABLE {alerts_table} AS SELECT * FROM alerts_df")
    conn.close()

    for a in alerts:
        logger.warning(f"[{a['severity']}] {a['message']}")
        print(f"[ALERTS] [{a['severity']:<6}] {a['message']}")

    if not alerts:
        print("[ALERTS] No anomalies detected.")
    else:
        print(f"[ALERTS] {len(alerts)} alert(s) saved → {ALERTS_REPORT_PATH}")

    return alerts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    check_alerts()
