import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing in .env")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("Connected to DB for cleaning.")

# -------------------------
# 1. Pull Raw Data
# -------------------------
query = "SELECT * FROM raw_hhs_facility;"
df = pd.read_sql(query, conn)

print(f"Raw rows pulled: {len(df)}")

# -------------------------
# 2. Type Normalization
# -------------------------
df["date"] = pd.to_datetime(df["collection_week"], errors="coerce")
df["icu_used"] = pd.to_numeric(df["icu_beds_used_7_day_avg"], errors="coerce")
df["icu_capacity"] = pd.to_numeric(df["total_icu_beds_7_day_avg"], errors="coerce")

# -------------------------
# 3. Structural Cleaning
# -------------------------
df = df.dropna(subset=["date", "icu_used", "icu_capacity"])
df = df[df["icu_capacity"] > 0]

# -------------------------
# 4. Compute Occupancy
# -------------------------
df["icu_occupancy_pct"] = df["icu_used"] / df["icu_capacity"]

# Remove impossible values
df = df[df["icu_occupancy_pct"] >= 0]
df = df[df["icu_occupancy_pct"] <= 1.5]

print(f"Rows after cleaning: {len(df)}")

# -------------------------
# 5. Keep Last 120 Weeks per Hospital
# -------------------------
df = df.sort_values("date")
df = df.groupby("hospital_pk").tail(120)

print(f"Rows after 120-week trim: {len(df)}")

# -------------------------
# 6. Select Clean Schema
# -------------------------
clean_df = df[[
    "hospital_pk",
    "hospital_name",
    "city",
    "zip",
    "fips_code",
    "date",
    "icu_used",
    "icu_capacity",
    "icu_occupancy_pct"
]]

# -------------------------
# 7. Upsert to Clean Table
# -------------------------
insert_query = """
INSERT INTO ma_icu_daily (
    hospital_pk,
    hospital_name,
    city,
    zip,
    fips_code,
    date,
    icu_used,
    icu_capacity,
    icu_occupancy_pct
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (hospital_pk, date)
DO UPDATE SET
    icu_used = EXCLUDED.icu_used,
    icu_capacity = EXCLUDED.icu_capacity,
    icu_occupancy_pct = EXCLUDED.icu_occupancy_pct;
"""

records = clean_df.values.tolist()

chunk_size = 1000
for i in range(0, len(records), chunk_size):
    cursor.executemany(insert_query, records[i:i+chunk_size])
    conn.commit()
    print(f"Upserted rows up to {min(i+chunk_size, len(records))}")

cursor.close()
conn.close()

print("Clean layer built successfully.")