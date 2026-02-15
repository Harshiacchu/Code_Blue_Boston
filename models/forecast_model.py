import pandas as pd

FEATURE_COLS = [
    "lag_1","lag_2","lag_3","lag_4",
    "roll_mean_4","roll_std_4","roll_min_4","roll_max_4",
    "delta_1","pct_change_1",
    "inpatient_occupancy_rate","covid_icu_burden_rate"
]

TARGET_COL = "icu_occupancy_rate"

def prepare_xy(features_df: pd.DataFrame):
    X = features_df[FEATURE_COLS].copy()
    y = features_df[TARGET_COL].copy()
    X = X.fillna(0.0)
    return X, y