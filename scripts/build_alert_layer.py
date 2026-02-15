import os
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

INITIAL_OXYGEN = 40
BURN_SCALE = 15

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Connected to DB for alert layer.")

    # ---------------------------
    # Pull latest clean data
    # ---------------------------
    df_clean = pd.read_sql(
        "SELECT * FROM ma_hospital_daily_clean;", conn
    )

    df_clean["icu_occupancy_rate"] = pd.to_numeric(
        df_clean["icu_occupancy_rate"], errors="coerce"
    )

    df_clean = df_clean.dropna(subset=["icu_occupancy_rate"])

    df_latest = (
        df_clean.sort_values(["hospital_pk", "date"], ascending=[True, False])
        .drop_duplicates(subset=["hospital_pk"], keep="first")
        .copy()
    )

    # ---------------------------
    # Pull forecast (if exists)
    # ---------------------------
    try:
        df_forecast = pd.read_sql(
            "SELECT * FROM ma_hospital_forecast;", conn
        )
    except:
        df_forecast = pd.DataFrame()

    if not df_forecast.empty:
        df_forecast["predicted_icu_occupancy"] = pd.to_numeric(
            df_forecast["predicted_icu_occupancy"], errors="coerce"
        )

        df_forecast = (
            df_forecast.sort_values(
                ["hospital_pk", "forecast_date"],
                ascending=[True, False]
            )
            .drop_duplicates(subset=["hospital_pk"], keep="first")
        )

        print("Forecast data detected.")
    else:
        print("No forecast found. Using current occupancy.")

    results = []

    for _, row in df_latest.iterrows():

        hospital_id = row["hospital_pk"]

        # ---------------------------
        # Determine occupancy (forecast or current)
        # ---------------------------
        if not df_forecast.empty and hospital_id in df_forecast["hospital_pk"].values:
            forecast_row = df_forecast[
                df_forecast["hospital_pk"] == hospital_id
            ].iloc[0]

            occupancy = float(forecast_row["predicted_icu_occupancy"])
        else:
            occupancy = float(row["icu_occupancy_rate"])

        occupancy = max(0.0, min(occupancy, 1.5))

        # ---------------------------
        # ICU Alert
        # ---------------------------
        if occupancy >= 0.9:
            icu_alert = "Critical"
        elif occupancy >= 0.75:
            icu_alert = "Watch"
        else:
            icu_alert = "Normal"

        # ---------------------------
        # Oxygen Simulation
        # ---------------------------
        burn_rate = occupancy * BURN_SCALE

        if burn_rate <= 0:
            weeks = 52
        else:
            oxygen = INITIAL_OXYGEN
            weeks = 0
            while oxygen > 0 and weeks < 52:
                oxygen -= burn_rate
                weeks += 1

        days_remaining = weeks * 7

        if weeks <= 3:
            oxygen_alert = "Critical"
        elif weeks <= 6:
            oxygen_alert = "Watch"
        else:
            oxygen_alert = "Normal"

        # ---------------------------
        # Final Severity
        # ---------------------------
        severity_rank = {
            "Normal": 0,
            "Watch": 1,
            "Critical": 2
        }

        final_alert = max(
            icu_alert,
            oxygen_alert,
            key=lambda x: severity_rank[x]
        )

        results.append((
            hospital_id,
            row["date"],
            days_remaining,
            weeks,
            oxygen_alert,
            icu_alert,
            final_alert,
            datetime.utcnow()
        ))

    # ---------------------------
    # Upsert results
    # ---------------------------
    insert_query = """
    INSERT INTO ma_hospital_alerts (
        hospital_pk,
        date,
        oxygen_days_remaining,
        oxygen_weeks_remaining,
        oxygen_alert_level,
        icu_alert_level,
        final_alert_level,
        created_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (hospital_pk, date)
    DO UPDATE SET
        oxygen_days_remaining = EXCLUDED.oxygen_days_remaining,
        oxygen_weeks_remaining = EXCLUDED.oxygen_weeks_remaining,
        oxygen_alert_level = EXCLUDED.oxygen_alert_level,
        icu_alert_level = EXCLUDED.icu_alert_level,
        final_alert_level = EXCLUDED.final_alert_level,
        created_at = EXCLUDED.created_at;
    """

    execute_batch(cursor, insert_query, results, page_size=1000)
    conn.commit()

    cursor.close()
    conn.close()

    print("Alert layer rebuilt successfully.")

if __name__ == "__main__":
    main()