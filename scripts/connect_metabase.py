"""Quick script to connect PostgreSQL warehouse to Metabase."""
import requests
import sys

METABASE = "http://localhost:3000"
EMAIL = "admin@cdp.local"
PASSWORD = "CDPAdmin2026!"

# Login
resp = requests.post(f"{METABASE}/api/session", json={"username": EMAIL, "password": PASSWORD}, timeout=10)
if resp.status_code != 200:
    print(f"Login failed ({resp.status_code}): {resp.text[:200]}")
    # Check if setup is needed
    props = requests.get(f"{METABASE}/api/session/properties", timeout=10).json()
    has_token = bool(props.get("setup-token"))
    has_user = props.get("has-user-setup")
    print(f"Has setup token: {has_token}, Has user setup: {has_user}")

    if has_token and not has_user:
        print("Running initial setup...")
        setup_payload = {
            "token": props["setup-token"],
            "prefs": {"site_name": "Customer Data Platform", "site_locale": "en", "allow_tracking": False},
            "user": {"first_name": "CDP", "last_name": "Admin", "email": EMAIL, "password": PASSWORD, "site_name": "Customer Data Platform"},
            "database": {
                "engine": "postgres",
                "name": "CDP Warehouse",
                "details": {"host": "postgres", "port": 5432, "dbname": "customer_data_platform", "user": "cdp_admin", "password": "cdp_secure_pass_2026", "ssl": False, "schema-filters-type": "inclusion", "schema-filters-patterns": ["warehouse", "analytics"]}
            }
        }
        r = requests.post(f"{METABASE}/api/setup", json=setup_payload, timeout=30)
        print(f"Setup result: {r.status_code} - {r.text[:300]}")
    sys.exit(1)

token = resp.json().get("id")
print(f"Logged in! Session: {token}")
headers = {"X-Metabase-Session": token}

# Check existing dbs
dbs = requests.get(f"{METABASE}/api/database", headers=headers, timeout=10).json()
db_names = [d.get("name") for d in dbs.get("data", [])]
print(f"Existing databases: {db_names}")

if "CDP Warehouse" not in db_names:
    db_payload = {
        "engine": "postgres",
        "name": "CDP Warehouse",
        "details": {"host": "postgres", "port": 5432, "dbname": "customer_data_platform", "user": "cdp_admin", "password": "cdp_secure_pass_2026", "ssl": False, "schema-filters-type": "inclusion", "schema-filters-patterns": ["warehouse", "analytics"]}
    }
    r = requests.post(f"{METABASE}/api/database", json=db_payload, headers=headers, timeout=30)
    print(f"Add DB: {r.status_code}")
    if r.status_code == 200:
        print("CDP Warehouse database added to Metabase!")
    else:
        print(f"Error: {r.text[:300]}")
else:
    print("CDP Warehouse already connected!")

# Trigger sync
for d in dbs.get("data", []):
    if d.get("name") == "CDP Warehouse":
        requests.post(f"{METABASE}/api/database/{d['id']}/sync_schema", headers=headers, timeout=10)
        print(f"Triggered schema sync for database id={d['id']}")
        break

print("\nMetabase is ready at http://localhost:3000")
print(f"Login: {EMAIL} / {PASSWORD}")
