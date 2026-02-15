import subprocess
import time
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def run_step(name, script):
    print(f"\n=== {name} ===")
    start = time.time()
    result = subprocess.run(["python", script], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{name} failed.")
    print(result.stdout)
    print(f"{name} completed in {round(time.time() - start, 2)}s")

def print_summary():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n=== FINAL SUMMARY ===")

    cur.execute("""
        SELECT COUNT(*)
        FROM ma_dashboard_final;
    """)
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT final_alert_level, COUNT(*)
        FROM ma_dashboard_final
        GROUP BY final_alert_level;
    """)
    distribution = cur.fetchall()

    print(f"Total Hospitals: {total}")
    print("Alert Distribution:")
    for row in distribution:
        print(f"  {row[0]}: {row[1]}")

    conn.close()

def main():
    print("\n🚀 Starting Full Pipeline Run")
    start_time = time.time()

    run_step("Build Clean Layer", "scripts/build_clean_layer.py")
    run_step("Build Stress Features", "scripts/build_stress_features.py")
    run_step("Train & Build Forecast", "scripts/build_forecast_layer.py")
    run_step("Build Overload Predictions", "scripts/build_overload_layer.py")
    run_step("Build Oxygen Alerts", "scripts/build_oxygen_alert_layer.py")

    print_summary()

    print(f"\n✅ Pipeline finished in {round(time.time() - start_time, 2)} seconds")

if __name__ == "__main__":
    main()