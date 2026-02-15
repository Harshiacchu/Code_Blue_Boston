import psycopg2
import random
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Generating simulated week...")

    # Get latest collection week
    cur.execute("""
        SELECT MAX(collection_week)
        FROM raw_hhs_facility;
    """)
    last_week = cur.fetchone()[0]

    new_week = last_week + timedelta(days=7)

    print(f"Last week: {last_week}")
    print(f"New week: {new_week}")

    # Pull all rows from latest week
    cur.execute("""
        SELECT *
        FROM raw_hhs_facility
        WHERE collection_week = %s;
    """, (last_week,))

    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    insert_query = f"""
        INSERT INTO raw_hhs_facility ({",".join(colnames)})
        VALUES ({",".join(["%s"] * len(colnames))});
    """

    for row in rows:
        row = list(row)

        # Update collection_week
        week_idx = colnames.index("collection_week")
        row[week_idx] = new_week

        # Modify ICU values slightly
        try:
            total_icu_idx = colnames.index("total_icu_beds_7_day_avg")
            used_icu_idx = colnames.index("icu_beds_used_7_day_avg")

            total_icu = row[total_icu_idx]
            used_icu = row[used_icu_idx]

            if total_icu and used_icu:
                noise = random.uniform(-0.05, 0.08)
                new_used = max(0, min(total_icu, used_icu * (1 + noise)))
                row[used_icu_idx] = new_used

        except ValueError:
            pass

        cur.execute(insert_query, tuple(row))

    conn.commit()
    conn.close()

    print("Simulation complete.")

if __name__ == "__main__":
    main()
