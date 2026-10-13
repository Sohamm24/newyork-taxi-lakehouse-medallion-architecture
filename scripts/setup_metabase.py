"""
setup_metabase.py
Automated provisioning script for Metabase BI dashboard.

Performs:
1. One-click initialization of Metabase admin credentials.
2. Registers PostgreSQL analytics database containing the synced Gold lakehouse tables.
3. Automatically creates the 4 core executive questions / charts:
   - Daily Trips & Gross Revenue Trend (Combo Line Chart)
   - Top 10 Pickup Zones by Volume & Revenue (Bar Chart)
   - Average Fare & Tip Percentage by Hour of Day (Line Chart)
   - Lakehouse Pipeline Health & Audit Log (Data Table)
4. Assembles them into an interactive "NYC Taxi Lakehouse Executive Analytics" dashboard.
"""

import time
import requests
import json
import os

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
HOST_URL = "http://localhost:3000"

ADMIN_FIRST_NAME = "Lakehouse"
ADMIN_LAST_NAME = "Admin"
ADMIN_EMAIL = "admin@lakehouse.local"
ADMIN_PASSWORD = "Admin123456!"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("AIRFLOW_DB_USER", "airflow")
PG_PASS = os.getenv("AIRFLOW_DB_PASSWORD", "airflow")
PG_DB = os.getenv("AIRFLOW_DB_NAME", "airflow")


def wait_for_metabase():
    urls = [METABASE_URL, HOST_URL]
    print(f"[METABASE SETUP] Checking Metabase availability...")
    for _ in range(45):
        for url in urls:
            try:
                res = requests.get(f"{url}/api/health", timeout=2)
                if res.status_code == 200:
                    print(f"[METABASE SETUP] Metabase is healthy and ready at {url}.")
                    return url
            except Exception:
                pass
        time.sleep(2)
    raise RuntimeError("Metabase failed to respond in time.")


def setup_metabase():
    base_url = wait_for_metabase()

    # 1. Check if setup is already complete
    session = requests.Session()
    props = session.get(f"{base_url}/api/session/properties").json()
    setup_token = props.get("setup-token")

    if not setup_token:
        print("[METABASE SETUP] Setup already completed. Logging in with existing credentials...")
        login_resp = session.post(f"{base_url}/api/session", json={
            "username": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if login_resp.status_code != 200:
            print(f"[METABASE SETUP] Could not log in: {login_resp.text}")
            return
        session_id = login_resp.json()["id"]
        session.headers.update({"X-Metabase-Session": session_id})
    else:
        print(f"[METABASE SETUP] Initializing Metabase with setup token {setup_token}...")
        setup_payload = {
            "token": setup_token,
            "user": {
                "first_name": ADMIN_FIRST_NAME,
                "last_name": ADMIN_LAST_NAME,
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "site_name": "NYC Taxi Lakehouse Executive Analytics"
            },
            "prefs": {
                "site_name": "NYC Taxi Lakehouse Executive Analytics",
                "site_locale": "en",
                "allow_tracking": False
            },
            "database": {
                "engine": "postgres",
                "name": "NYC Taxi Lakehouse (Postgres)",
                "details": {
                    "host": PG_HOST,
                    "port": PG_PORT,
                    "dbname": PG_DB,
                    "user": PG_USER,
                    "password": PG_PASS,
                    "ssl": False
                },
                "is_full_sync": True
            }
        }
        res = session.post(f"{base_url}/api/setup", json=setup_payload)
        if res.status_code not in (200, 201):
            print(f"[METABASE SETUP] Setup error: {res.text}")
            return
        session_id = res.json()["id"]
        session.headers.update({"X-Metabase-Session": session_id})
        print("[METABASE SETUP] Admin user and Postgres database created successfully.")

    # 2. Find Database ID
    db_list = session.get(f"{base_url}/api/database").json()["data"]
    db_id = None
    for db in db_list:
        if db["engine"] == "postgres":
            db_id = db["id"]
            break

    if not db_id:
        print("[METABASE SETUP] Adding Postgres Lakehouse database connection...")
        add_db_res = session.post(f"{base_url}/api/database", json={
            "engine": "postgres",
            "name": "NYC Taxi Lakehouse (Postgres)",
            "details": {
                "host": PG_HOST,
                "port": PG_PORT,
                "dbname": PG_DB,
                "user": PG_USER,
                "password": PG_PASS,
                "ssl": False
            },
            "is_full_sync": True
        })
        db_id = add_db_res.json()["id"]

    print(f"[METABASE SETUP] Using Database ID: {db_id}")
    session.post(f"{base_url}/api/database/{db_id}/sync_schema")
    time.sleep(2)

    # 3. Create Dashboard
    dashboards = session.get(f"{base_url}/api/dashboard").json()
    dash_id = None
    for d in dashboards:
        if d.get("name") == "NYC Taxi Lakehouse Executive Analytics":
            dash_id = d["id"]
            break

    if not dash_id:
        print("[METABASE SETUP] Creating Executive Analytics Dashboard...")
        dash_res = session.post(f"{base_url}/api/dashboard", json={
            "name": "NYC Taxi Lakehouse Executive Analytics",
            "description": "Real-time Lakehouse KPIs: Daily Revenue, Top Pickup Zones, Hourly Fares, and Pipeline Health."
        })
        dash_id = dash_res.json()["id"]

    print(f"[METABASE SETUP] Dashboard ID: {dash_id}")

    # 4. Create the 4 core cards/questions
    queries = [
        {
            "name": "Daily Trips & Gross Revenue Trend",
            "description": "Daily aggregate trip volume and total revenue generated across NYC yellow taxis.",
            "display": "line",
            "sql": "SELECT pickup_date, total_trips, total_revenue FROM gold_daily_trips_revenue ORDER BY pickup_date ASC;",
            "visualization_settings": {
                "graph.dimensions": ["pickup_date"],
                "graph.metrics": ["total_trips", "total_revenue"],
                "series_settings": {
                    "total_revenue": {"title": "Gross Revenue ($)"},
                    "total_trips": {"title": "Total Trips"}
                }
            },
            "layout": {"col": 0, "row": 0, "size_x": 12, "size_y": 7}
        },
        {
            "name": "Top 10 Pickup Zones by Volume & Revenue",
            "description": "Highest performing taxi pickup zones in NYC with total volume, revenue, and average fare.",
            "display": "bar",
            "sql": "SELECT zone, borough, total_trips, total_revenue, avg_fare FROM gold_pickup_zone_metrics ORDER BY total_trips DESC LIMIT 10;",
            "visualization_settings": {
                "graph.dimensions": ["zone"],
                "graph.metrics": ["total_trips"]
            },
            "layout": {"col": 12, "row": 0, "size_x": 12, "size_y": 7}
        },
        {
            "name": "Average Fare & Tip Percentage by Hour of Day",
            "description": "Hourly distribution of average taxi fares and passenger tip percentages (0-23 hours).",
            "display": "line",
            "sql": "SELECT pickup_hour, avg_fare, avg_tip_percent, total_trips FROM gold_hourly_fare_tip_metrics ORDER BY pickup_hour ASC;",
            "visualization_settings": {
                "graph.dimensions": ["pickup_hour"],
                "graph.metrics": ["avg_fare", "avg_tip_percent"]
            },
            "layout": {"col": 0, "row": 7, "size_x": 12, "size_y": 7}
        },
        {
            "name": "Data Lakehouse Pipeline Health & Audit Log",
            "description": "Audit trail showing latest pipeline runs, rows processed, quarantine counts, and execution status.",
            "display": "table",
            "sql": "SELECT run_id, layer, target_table, rows_in, rows_out, rows_rejected, run_timestamp, status FROM pipeline_audit ORDER BY run_timestamp DESC LIMIT 15;",
            "visualization_settings": {},
            "layout": {"col": 12, "row": 7, "size_x": 12, "size_y": 7}
        }
    ]

    # Fetch existing cards
    cards = session.get(f"{base_url}/api/card").json()
    existing_cards = {c["name"]: c["id"] for c in cards}

    dashcards_to_add = []
    for q in queries:
        card_id = existing_cards.get(q["name"])
        if not card_id:
            print(f"[METABASE SETUP] Creating Question: {q['name']}...")
            card_payload = {
                "name": q["name"],
                "description": q["description"],
                "display": q["display"],
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": q["sql"]
                    },
                    "database": db_id
                },
                "visualization_settings": q["visualization_settings"]
            }
            c_res = session.post(f"{base_url}/api/card", json=card_payload)
            if c_res.status_code in (200, 201):
                card_id = c_res.json()["id"]
                print(f"  -> Created card ID {card_id}")
            else:
                print(f"  -> Failed to create card: {c_res.text}")
                continue

        dashcards_to_add.append({
            "card_id": card_id,
            "col": q["layout"]["col"],
            "row": q["layout"]["row"],
            "size_x": q["layout"]["size_x"],
            "size_y": q["layout"]["size_y"],
            "parameter_mappings": []
        })

    # Add cards to dashboard
    current_dash = session.get(f"{base_url}/api/dashboard/{dash_id}").json()
    current_dashcard_card_ids = [dc.get("card_id") for dc in current_dash.get("dashcards", []) if dc.get("card_id")]

    # Update dashboard cards
    new_dashcards = []
    for item in dashcards_to_add:
        if item["card_id"] not in current_dashcard_card_ids:
            new_dashcards.append({
                "id": -1 * len(new_dashcards) - 1,
                "card_id": item["card_id"],
                "col": item["col"],
                "row": item["row"],
                "size_x": item["size_x"],
                "size_y": item["size_y"]
            })

    if new_dashcards:
        print("[METABASE SETUP] Pinning cards to Dashboard layout...")
        dash_update_res = session.put(f"{base_url}/api/dashboard/{dash_id}/cards", json={
            "cards": new_dashcards
        })
        if dash_update_res.status_code == 200:
            print("[METABASE SETUP] Dashboard updated successfully!")

    print("\n" + "=" * 70)
    print("METABASE EXECUTIVE DASHBOARD PROVISIONED SUCCESSFULLY!")
    print(f"URL:      http://localhost:3000/dashboard/{dash_id}")
    print(f"Username: {ADMIN_EMAIL}")
    print(f"Password: {ADMIN_PASSWORD}")
    print("=" * 70)


if __name__ == "__main__":
    setup_metabase()
