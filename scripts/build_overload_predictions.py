import os
import joblib
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score

# ---------------------------
# Config
# ---------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

MODEL_PATH = "models/rf_overload_v1.pkl"
MODEL_VERSION = "rf_overload_v1"

RETRAIN_MODEL = True

# Engineer feature set
FEATURE_COLS = [
    "stress_lag1",
    "stress_lag2",
    "stress_roll3",
    "stress_roll6",
    "stress_roll6_std",
    "icu_util",
    "icu_delta_1w",
    "oxygen_risk_proxy",
    "stress_centered"
]

# stress threshold used by engineer
STRESS_OVERLOAD_THRESHOLD = 75.0

# probability threshold for overload flag
# keep fixed for stability; you can later tune
PROB_THRESHOLD = 0.35

# ---------------------------
# Helpers
# ---------------------------
def prepare_xy(df: pd.DataFrame):
    df = df.copy()

    # Ensure numeric
    for col in FEATURE_COLS + ["stress_next"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Need stress_next to create label
    df = df.dropna(subset=["stress_next"])

    df["overload_next"] = (df["stress_next"] >= STRESS_OVERLOAD_THRESHOLD).astype(int)

    # Need minimum lag features
    df = df.dropna(subset=["stress_lag2"])

    X = df[FEATURE_COLS].fillna(0.0)
    y = df["overload_next"].astype(int)

    return X, y, df


def train_and_eval_hospital_holdout(df: pd.DataFrame):
    """
    Hospital holdout (like your regression model).
    Prevents training & validation mixing across hospitals.
    """

    hospitals = df["hospital_pk"].dropna().unique()
    if len(hospitals) < 5:
        raise ValueError("Not enough hospitals for holdout split.")

    train_h, val_h = train_test_split(
        hospitals, test_size=0.2, random_state=42
    )

    train_df = df[df["hospital_pk"].isin(train_h)]
    val_df = df[df["hospital_pk"].isin(val_h)]

    X_train, y_train, _ = prepare_xy(train_df)
    X_val, y_val, _ = prepare_xy(val_df)

    model = RandomForestClassifier(
        n_estimators=500,
        max_depth=12,
        min_samples_leaf=10,
        class_weight={0: 1, 1: 2},
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    probs = model.predict_proba(X_val)[:, 1]
    preds = (probs >= PROB_THRESHOLD).astype(int)

    acc = accuracy_score(y_val, preds)
    prec = precision_score(y_val, preds, zero_division=0)
    rec = recall_score(y_val, preds, zero_division=0)

    # roc-auc can fail if only one class exists in y_val
    try:
        auc = roc_auc_score(y_val, probs)
    except ValueError:
        auc = float("nan")

    print("\n===== Overload Classifier (Hospital Holdout) =====")
    print(f"Train hospitals: {len(train_h)} | Val hospitals: {len(val_h)}")
    print(f"Stress threshold (label): {STRESS_OVERLOAD_THRESHOLD}")
    print(f"Prob threshold (flag): {PROB_THRESHOLD}")
    print(f"Accuracy : {acc:.4f}")
    print(f"Precision: {prec:.4f}")
    print(f"Recall   : {rec:.4f}")
    print(f"ROC-AUC  : {auc:.4f}" if not np.isnan(auc) else "ROC-AUC  : NA (single-class val)")
    print("==================================================\n")

    return model


def load_or_train(df: pd.DataFrame):
    if (not RETRAIN_MODEL) and os.path.exists(MODEL_PATH):
        print("Loading existing overload model...")
        return joblib.load(MODEL_PATH)

    print("Training RandomForest overload model...")
    model = train_and_eval_hospital_holdout(df)

    os.makedirs("models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    return model


# ---------------------------
# Main
# ---------------------------
def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Connected to DB for overload predictions.")

    # Pull stress features
    df = pd.read_sql("SELECT * FROM ma_stress_features;", conn)
    if df.empty:
        raise ValueError("ma_stress_features is empty. Run build_stress_features.py first.")

    # Make sure date is valid and ordered
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["hospital_pk", "date"])
    df = df.sort_values(["hospital_pk", "date"])

    model = load_or_train(df)

    # Build one-step-ahead (next week) overload prediction per hospital
    results = []

    for hospital_pk, hdf in df.groupby("hospital_pk"):
        hdf = hdf.sort_values("date")

        latest = hdf.iloc[-1].copy()

        # Need features present; if missing, skip
        feat = latest[FEATURE_COLS]
        feat = pd.to_numeric(feat, errors="coerce").fillna(0.0).values.reshape(1, -1)

        prob = float(model.predict_proba(feat)[0][1])
        flag = int(prob >= PROB_THRESHOLD)

        forecast_date = (pd.to_datetime(latest["date"]) + timedelta(weeks=1)).date()

        results.append((
            str(hospital_pk),
            forecast_date,
            prob,
            flag,
            float(PROB_THRESHOLD),
            MODEL_VERSION,
            datetime.utcnow()
        ))

    # Upsert into table
    insert_query = """
    INSERT INTO ma_overload_predictions (
        hospital_pk,
        forecast_date,
        overload_probability,
        overload_flag,
        threshold_used,
        model_version,
        created_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (hospital_pk, forecast_date)
    DO UPDATE SET
        overload_probability = EXCLUDED.overload_probability,
        overload_flag = EXCLUDED.overload_flag,
        threshold_used = EXCLUDED.threshold_used,
        model_version = EXCLUDED.model_version,
        created_at = EXCLUDED.created_at;
    """

    execute_batch(cur, insert_query, results, page_size=1000)
    conn.commit()

    cur.close()
    conn.close()

    print("Overload predictions built successfully.")


if __name__ == "__main__":
    main()