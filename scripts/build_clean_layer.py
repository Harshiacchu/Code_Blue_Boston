import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ---------------------------
# Load environment
# ---------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("Connected to DB for cleaning.")

# ---------------------------
# Pull raw data
# ---------------------------
df = pd.read_sql("SELECT * FROM raw_hhs_facility;", conn)

# ---------------------------
# Convert date
# ---------------------------
df["date"] = pd.to_datetime(df["collection_week"], errors="coerce")
df = df.dropna(subset=["date"])

# ---------------------------
# Detect numeric columns from DB
# ---------------------------
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'raw_hhs_facility'
    AND data_type = 'double precision';
""")

numeric_cols = [row[0] for row in cursor.fetchall()]

# ---------------------------
# Normalize numeric fields
# ---------------------------
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ---------------------------
# Structural guards
# ---------------------------
if "total_icu_beds_7_day_avg" in df.columns:
    df = df[df["total_icu_beds_7_day_avg"] > 0]

if "inpatient_beds_7_day_avg" in df.columns:
    df = df[df["inpatient_beds_7_day_avg"] > 0]

# ---------------------------
# Keep last 120 weeks per hospital
# ---------------------------
df = df.sort_values("date")
df = df.groupby("hospital_pk").tail(120)

print(f"Rows after cleaning: {len(df)}")

# ---------------------------
# Prepare clean table insert
# ---------------------------

# Get clean table columns
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'ma_hospital_daily_clean';
""")

clean_cols = [row[0] for row in cursor.fetchall()]

# Ensure only matching columns are inserted
df = df[[col for col in df.columns if col in clean_cols]]

cols = df.columns.tolist()
placeholders = ",".join(["%s"] * len(cols))

insert_query = f"""
INSERT INTO ma_hospital_daily_clean ({",".join(cols)})
VALUES ({placeholders})
ON CONFLICT (hospital_pk, date)
DO UPDATE SET
{",".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ["hospital_pk", "date"]])};
"""

records = df.to_dict("records")

# ---------------------------
# Insert in chunks
# ---------------------------
chunk_size = 1000

for i in range(0, len(records), chunk_size):
    values = [
        tuple(record[col] for col in cols)
        for record in records[i:i+chunk_size]
    ]
    cursor.executemany(insert_query, values)
    conn.commit()
    print(f"Upserted up to {min(i + chunk_size, len(records))}")

cursor.close()
conn.close()

print("Clean layer built successfully.")