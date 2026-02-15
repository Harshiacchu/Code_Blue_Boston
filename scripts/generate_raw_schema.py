import pandas as pd

df = pd.read_csv("data/raw/hhs_hospital_capacity.csv", low_memory=False)
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace("-", "_")
df.columns = df.columns.str.replace(" ", "_")

print("create table raw_hhs_facility (")

for col in df.columns:
    if col == "collection_week":
        print(f"  {col} date,")
    else:
        print(f"  {col} double precision,")

print(");")