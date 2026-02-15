import os
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def clamp01(x):
    return np.clip(x, 0.0, 1.0)

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Building ma_stress_features...")

    df = pd.read_sql("SELECT * FROM ma_hospital_daily_clean;", conn)
    if df.empty:
        raise ValueError("ma_hospital_daily_clean is empty")

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["hospital_pk", "date"])

    # Ensure numeric
    # Ensure numeric
    ratio_cols = [
        "icu_occupancy_rate",
        "inpatient_occupancy_rate",
        "covid_icu_burden_rate"
    ]

    for c in ratio_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Guards
    df = df.dropna(subset=["icu_occupancy_rate"])
    df = df[df["icu_occupancy_rate"] >= 0]

    # --- Core ratios (already engineered in clean layer) ---
    df["icu_util"] = clamp01(df["icu_occupancy_rate"])
    df["inpatient_util"] = clamp01(df["inpatient_occupancy_rate"])
    df["covid_ratio"] = clamp01(df["covid_icu_burden_rate"])

    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)

    # --- Trend Features ---
    df = df.sort_values(["hospital_pk", "date"])
    g = df.groupby("hospital_pk")

    df["icu_util_lag1"] = g["icu_util"].shift(1)
    df["icu_util_lag2"] = g["icu_util"].shift(2)

    df["icu_delta_1w"] = (df["icu_util"] - df["icu_util_lag1"]).clip(-0.3, 0.3)
    df["icu_delta_2w"] = (df["icu_util"] - df["icu_util_lag2"]).clip(-0.3, 0.3)

    # --- Oxygen Proxy ---
    df["oxygen_risk_proxy"] = clamp01(
        0.6 * df["icu_util"] +
        0.2 * df["inpatient_util"] +
        0.2 * df["covid_ratio"]
    )

    # --- Stress Score ---
    df["stress"] = (
        0.5 * df["icu_util"] * 100 +
        0.3 * df["oxygen_risk_proxy"] * 100 +
        0.2 * clamp01(df["icu_delta_1w"].fillna(0) / 0.10) * 100
    )

    df["stress_next"] = g["stress"].shift(-1)

    # --- Stress History ---
    df["stress_lag1"] = g["stress"].shift(1)
    df["stress_lag2"] = g["stress"].shift(2)
    df["stress_delta_1w"] = df["stress"] - df["stress_lag1"]

    df["stress_roll3"] = g["stress"].transform(lambda x: x.shift(1).rolling(3).mean())
    df["stress_roll6"] = g["stress"].transform(lambda x: x.shift(1).rolling(6).mean())
    df["stress_roll6_std"] = g["stress"].transform(lambda x: x.shift(1).rolling(6).std())

    hospital_mean = g["stress"].transform("mean")
    df["stress_centered"] = df["stress"] - hospital_mean
    df["stress_next_centered"] = df["stress_next"] - hospital_mean

    # --- Seasonality ---
    df["weekofyear"] = df["date"].dt.isocalendar().week.astype(int)
    df["weekofyear_sin"] = np.sin(2 * np.pi * df["weekofyear"] / 52.0)
    df["weekofyear_cos"] = np.cos(2 * np.pi * df["weekofyear"] / 52.0)

    # --- Output ---
    df_out = df[[
        "hospital_pk","date",
        "icu_util","inpatient_util","covid_ratio",
        "icu_util_lag1","icu_util_lag2","icu_delta_1w","icu_delta_2w",
        "oxygen_risk_proxy",
        "stress","stress_next",
        "stress_lag1","stress_lag2","stress_delta_1w",
        "stress_roll3","stress_roll6","stress_roll6_std",
        "stress_centered","stress_next_centered",
        "weekofyear","weekofyear_sin","weekofyear_cos"
    ]].copy()

    df_out["created_at"] = datetime.utcnow()

    records = list(df_out.itertuples(index=False, name=None))

    insert = """
    INSERT INTO ma_stress_features (
      hospital_pk, date,
      icu_util, inpatient_util, covid_ratio,
      icu_util_lag1, icu_util_lag2, icu_delta_1w, icu_delta_2w,
      oxygen_risk_proxy,
      stress, stress_next,
      stress_lag1, stress_lag2, stress_delta_1w,
      stress_roll3, stress_roll6, stress_roll6_std,
      stress_centered, stress_next_centered,
      weekofyear, weekofyear_sin, weekofyear_cos,
      created_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (hospital_pk, date)
    DO UPDATE SET
      icu_util=EXCLUDED.icu_util,
      inpatient_util=EXCLUDED.inpatient_util,
      covid_ratio=EXCLUDED.covid_ratio,
      icu_util_lag1=EXCLUDED.icu_util_lag1,
      icu_util_lag2=EXCLUDED.icu_util_lag2,
      icu_delta_1w=EXCLUDED.icu_delta_1w,
      icu_delta_2w=EXCLUDED.icu_delta_2w,
      oxygen_risk_proxy=EXCLUDED.oxygen_risk_proxy,
      stress=EXCLUDED.stress,
      stress_next=EXCLUDED.stress_next,
      stress_lag1=EXCLUDED.stress_lag1,
      stress_lag2=EXCLUDED.stress_lag2,
      stress_delta_1w=EXCLUDED.stress_delta_1w,
      stress_roll3=EXCLUDED.stress_roll3,
      stress_roll6=EXCLUDED.stress_roll6,
      stress_roll6_std=EXCLUDED.stress_roll6_std,
      stress_centered=EXCLUDED.stress_centered,
      stress_next_centered=EXCLUDED.stress_next_centered,
      weekofyear=EXCLUDED.weekofyear,
      weekofyear_sin=EXCLUDED.weekofyear_sin,
      weekofyear_cos=EXCLUDED.weekofyear_cos,
      created_at=EXCLUDED.created_at;
    """

    execute_batch(cur, insert, records, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()

    print("ma_stress_features built successfully.")

if __name__ == "__main__":
    main()