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
    raise ValueError("DATABASE_URL not found in .env")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("Connected to DB.")

# ---------------------------
# Load CSV
# ---------------------------
df = pd.read_csv(
    "data/raw/hhs_hospital_capacity.csv",
    low_memory=False
)

# ---------------------------
# Normalize column names
# ---------------------------
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace("-", "_", regex=False)
df.columns = df.columns.str.replace("+", "", regex=False)
df.columns = df.columns.str.replace(" ", "_", regex=False)

# ---------------------------
# Filter Massachusetts only
# ---------------------------
df = df[df["state"] == "MA"]

# ---------------------------
# Replace sentinel values
# ---------------------------
df.replace("-999,999", None, inplace=True)
df.replace(-999999, None, inplace=True)

# ---------------------------
# Convert collection_week to date
# ---------------------------
df["collection_week"] = pd.to_datetime(
    df["collection_week"], errors="coerce"
)
df = df.dropna(subset=["collection_week"])

# ---------------------------
# Get DB numeric columns
# ---------------------------
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'raw_hhs_facility'
    AND data_type = 'double precision';
""")

numeric_cols = [row[0] for row in cursor.fetchall()]

# ---------------------------
# Clean numeric formatting
# ---------------------------
for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace("NaN", None)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ---------------------------
# Ensure only DB columns are inserted
# ---------------------------
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_name = 'raw_hhs_facility';
""")

db_cols = [row[0] for row in cursor.fetchall()]

df = df[[col for col in df.columns if col in db_cols]]

print(f"Columns being inserted: {len(df.columns)}")

records = df.to_dict("records")
cols = df.columns.tolist()
placeholders = ",".join(["%s"] * len(cols))

insert_query = f"""
INSERT INTO raw_hhs_facility ({",".join(cols)})
VALUES ({placeholders});
"""

# ---------------------------
# Insert in chunks
# ---------------------------
chunk_size = 500

for i in range(0, len(records), chunk_size):
    values = [
        tuple(record[col] for col in cols)
        for record in records[i:i+chunk_size]
    ]
    cursor.executemany(insert_query, values)
    conn.commit()
    print(f"Inserted up to {min(i + chunk_size, len(records))}")

cursor.close()
conn.close()

print("Raw ingestion complete.")