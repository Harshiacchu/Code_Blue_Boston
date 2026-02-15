import os
import joblib
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, GridSearchCV
from xgboost import XGBRegressor

# ---------------------------
# Config
# ---------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

MODEL_PATH = "models/xgb_forecast_v1.pkl"
MODEL_VERSION = "xgb_v1_group_holdout"

RETRAIN_MODEL = True
SCENARIO_MULTIPLIER = 1.0

FEATURE_COLS = [
    "lag_1","lag_2","lag_3","lag_4",
    "roll_mean_4","roll_std_4","roll_min_4","roll_max_4",
    "delta_1","pct_change_1",
    "inpatient_occupancy_rate","covid_icu_burden_rate"
]

TARGET_COL = "icu_occupancy_rate"

# ---------------------------
# Utilities
# ---------------------------
def prepare_xy(df):
    df = df.copy()

    # Ensure datetime safety
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Force numeric conversion
    for col in FEATURE_COLS + [TARGET_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=[TARGET_COL])

    X = df[FEATURE_COLS].fillna(0.0)
    y = df[TARGET_COL]

    return X, y


# ---------------------------
# Hospital Holdout Training
# ---------------------------
def train_and_evaluate_group_holdout(df_features):

    hospitals = df_features["hospital_pk"].unique()

    train_hospitals, val_hospitals = train_test_split(
        hospitals, test_size=0.2, random_state=42
    )

    train_df = df_features[df_features["hospital_pk"].isin(train_hospitals)]
    val_df = df_features[df_features["hospital_pk"].isin(val_hospitals)]

    X_train, y_train = prepare_xy(train_df)
    X_val, y_val = prepare_xy(val_df)

    print("\nFeature dtypes:")
    print(X_train.dtypes)

    base_model = XGBRegressor(
        objective="reg:squarederror",
        random_state=42
    )

    param_grid = {
        "max_depth": [3,4],
        "learning_rate": [0.03, 0.05],
        "n_estimators": [200,300],
        "subsample": [0.8],
        "colsample_bytree": [0.8]
    }

    grid = GridSearchCV(
        base_model,
        param_grid,
        cv=3,
        scoring="neg_mean_absolute_error",
        n_jobs=-1
    )

    grid.fit(X_train, y_train)

    model = grid.best_estimator_

    y_pred = model.predict(X_val)

    mae = mean_absolute_error(y_val, y_pred)
    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    r2 = r2_score(y_val, y_pred)

    print("\n===== Hospital Holdout Evaluation =====")
    print(f"Train hospitals: {len(train_hospitals)}")
    print(f"Validation hospitals: {len(val_hospitals)}")
    print(f"Best Params: {grid.best_params_}")
    print(f"MAE  : {mae:.4f}")
    print(f"RMSE : {rmse:.4f}")
    print(f"R²   : {r2:.4f}")
    print("=======================================\n")

    return model


def load_or_train_model(df_features):
    if not RETRAIN_MODEL and os.path.exists(MODEL_PATH):
        print("Loading existing model...")
        return joblib.load(MODEL_PATH)

    print("Training XGBoost model with hospital holdout...")
    model = train_and_evaluate_group_holdout(df_features)

    os.makedirs("models", exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")

    return model


# ---------------------------
# Recursive Forecast
# ---------------------------
def recursive_forecast(model, hospital_df, steps=4):

    forecasts = []

    hospital_df = hospital_df.copy()
    hospital_df["date"] = pd.to_datetime(hospital_df["date"], errors="coerce")
    hospital_df = hospital_df.sort_values("date")

    latest_row = hospital_df.iloc[-1].copy()
    current_date = pd.to_datetime(latest_row["date"])

    for step in range(1, steps + 1):

        X = latest_row[FEATURE_COLS].fillna(0.0).values.reshape(1, -1)
        pred = float(model.predict(X)[0])

        pred *= SCENARIO_MULTIPLIER
        pred = max(0.0, min(pred, 1.2))

        forecast_date = current_date + timedelta(weeks=step)
        forecasts.append((forecast_date.date(), pred))

        # Update lag chain
        latest_row["lag_4"] = latest_row["lag_3"]
        latest_row["lag_3"] = latest_row["lag_2"]
        latest_row["lag_2"] = latest_row["lag_1"]
        latest_row["lag_1"] = pred

        lags = [
            latest_row["lag_1"],
            latest_row["lag_2"],
            latest_row["lag_3"],
            latest_row["lag_4"],
        ]

        latest_row["roll_mean_4"] = np.mean(lags)
        latest_row["roll_std_4"] = np.std(lags)
        latest_row["roll_min_4"] = np.min(lags)
        latest_row["roll_max_4"] = np.max(lags)
        latest_row["delta_1"] = latest_row["lag_1"] - latest_row["lag_2"]

        if latest_row["lag_2"] != 0:
            latest_row["pct_change_1"] = (
                latest_row["lag_1"] - latest_row["lag_2"]
            ) / latest_row["lag_2"]
        else:
            latest_row["pct_change_1"] = 0.0

    return forecasts


# ---------------------------
# Main
# ---------------------------
def main():

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Connected to DB for forecasting.")

    df_features = pd.read_sql("SELECT * FROM ma_hospital_features;", conn)

    if df_features.empty:
        raise ValueError("Feature table empty")

    model = load_or_train_model(df_features)

    results = []

    for hospital_id, hospital_df in df_features.groupby("hospital_pk"):

        forecasts = recursive_forecast(model, hospital_df, steps=4)

        for forecast_date, pred_value in forecasts:
            results.append((
                hospital_id,
                forecast_date,
                float(pred_value),
                MODEL_VERSION,
                datetime.utcnow()
            ))

    insert_query = """
    INSERT INTO ma_hospital_forecast (
        hospital_pk,
        forecast_date,
        predicted_icu_occupancy,
        model_version,
        created_at
    )
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (hospital_pk, forecast_date)
    DO UPDATE SET
        predicted_icu_occupancy = EXCLUDED.predicted_icu_occupancy,
        model_version = EXCLUDED.model_version,
        created_at = EXCLUDED.created_at;
    """

    execute_batch(cursor, insert_query, results, page_size=1000)
    conn.commit()

    cursor.close()
    conn.close()

    print("4-week forecast layer built successfully.")


if __name__ == "__main__":
    main()