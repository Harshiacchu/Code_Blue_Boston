import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing in .env")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("Connected to Supabase Postgres.")

# Load CSV
df = pd.read_csv("data/raw/hhs_hospital_capacity.csv")
df.columns = df.columns.str.lower()

# Filter MA only
df = df[df["state"] == "MA"]

# Select required columns
df = df[[
    "hospital_pk",
    "hospital_name",
    "state",
    "city",
    "zip",
    "fips_code",
    "collection_week",
    "total_icu_beds_7_day_avg",
    "icu_beds_used_7_day_avg"
]]

# Replace sentinel values
df.replace("-999,999", None, inplace=True)
df.replace(-999999, None, inplace=True)

# Convert ICU fields to numeric
df["total_icu_beds_7_day_avg"] = pd.to_numeric(
    df["total_icu_beds_7_day_avg"], errors="coerce"
)

df["icu_beds_used_7_day_avg"] = pd.to_numeric(
    df["icu_beds_used_7_day_avg"], errors="coerce"
)

# Drop rows with missing ICU values
df = df.dropna(subset=[
    "total_icu_beds_7_day_avg",
    "icu_beds_used_7_day_avg"
])

records = df.values.tolist()

insert_query = """
INSERT INTO raw_hhs_facility (
    hospital_pk,
    hospital_name,
    state,
    city,
    zip,
    fips_code,
    collection_week,
    total_icu_beds_7_day_avg,
    icu_beds_used_7_day_avg
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

# Insert in chunks
chunk_size = 1000
for i in range(0, len(records), chunk_size):
    cursor.executemany(insert_query, records[i:i+chunk_size])
    conn.commit()
    print(f"Inserted {i + chunk_size} rows")

cursor.close()
conn.close()

print("Raw ingestion complete.")