import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found")

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["hospital_pk", "date"])

    # numeric safety
    for c in ["icu_occupancy_rate", "inpatient_occupancy_rate", "covid_icu_burden_rate"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    g = df.groupby("hospital_pk", group_keys=False)

    # lags
    for k in [1, 2, 3, 4]:
        df[f"lag_{k}"] = g["icu_occupancy_rate"].shift(k)

    # rolling (use past only)
    df["roll_mean_4"] = g["icu_occupancy_rate"].shift(1).rolling(4).mean()
    df["roll_std_4"]  = g["icu_occupancy_rate"].shift(1).rolling(4).std()
    df["roll_min_4"]  = g["icu_occupancy_rate"].shift(1).rolling(4).min()
    df["roll_max_4"]  = g["icu_occupancy_rate"].shift(1).rolling(4).max()

    # dynamics
    df["delta_1"] = df["icu_occupancy_rate"] - df["lag_1"]
    df["pct_change_1"] = (df["icu_occupancy_rate"] - df["lag_1"]) / df["lag_1"]

    # clean infinities
    df.replace([float("inf"), float("-inf")], pd.NA, inplace=True)

    # drop rows where we cannot train/predict because no lag history
    df = df.dropna(subset=["lag_1", "roll_mean_4"])

    df["created_at"] = datetime.utcnow()

    keep = [
        "hospital_pk", "date",
        "icu_occupancy_rate",
        "inpatient_occupancy_rate", "covid_icu_burden_rate",
        "lag_1", "lag_2", "lag_3", "lag_4",
        "roll_mean_4", "roll_std_4", "roll_min_4", "roll_max_4",
        "delta_1", "pct_change_1",
        "created_at"
    ]
    return df[keep]

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    print("Connected to DB for feature engineering.")

    df = pd.read_sql("SELECT * FROM ma_hospital_daily_clean;", conn)
    if df.empty:
        raise ValueError("ma_hospital_daily_clean is empty")

    feats = build_features(df)
    print("Feature rows:", len(feats))

    # upsert
    insert_query = """
    INSERT INTO ma_hospital_features (
        hospital_pk, date,
        icu_occupancy_rate,
        inpatient_occupancy_rate, covid_icu_burden_rate,
        lag_1, lag_2, lag_3, lag_4,
        roll_mean_4, roll_std_4, roll_min_4, roll_max_4,
        delta_1, pct_change_1,
        created_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (hospital_pk, date)
    DO UPDATE SET
        icu_occupancy_rate = EXCLUDED.icu_occupancy_rate,
        inpatient_occupancy_rate = EXCLUDED.inpatient_occupancy_rate,
        covid_icu_burden_rate = EXCLUDED.covid_icu_burden_rate,
        lag_1 = EXCLUDED.lag_1,
        lag_2 = EXCLUDED.lag_2,
        lag_3 = EXCLUDED.lag_3,
        lag_4 = EXCLUDED.lag_4,
        roll_mean_4 = EXCLUDED.roll_mean_4,
        roll_std_4 = EXCLUDED.roll_std_4,
        roll_min_4 = EXCLUDED.roll_min_4,
        roll_max_4 = EXCLUDED.roll_max_4,
        delta_1 = EXCLUDED.delta_1,
        pct_change_1 = EXCLUDED.pct_change_1,
        created_at = EXCLUDED.created_at;
    """

    rows = [
        (
            r.hospital_pk, r.date.date(),
            float(r.icu_occupancy_rate),
            float(r.inpatient_occupancy_rate) if pd.notna(r.inpatient_occupancy_rate) else None,
            float(r.covid_icu_burden_rate) if pd.notna(r.covid_icu_burden_rate) else None,
            float(r.lag_1), float(r.lag_2), float(r.lag_3), float(r.lag_4),
            float(r.roll_mean_4),
            float(r.roll_std_4) if pd.notna(r.roll_std_4) else None,
            float(r.roll_min_4),
            float(r.roll_max_4),
            float(r.delta_1),
            float(r.pct_change_1) if pd.notna(r.pct_change_1) else None,
            r.created_at
        )
        for r in feats.itertuples(index=False)
    ]

    execute_batch(cursor, insert_query, rows, page_size=1000)
    conn.commit()
    cursor.close()
    conn.close()
    print("Feature layer built successfully.")

if __name__ == "__main__":
    main()