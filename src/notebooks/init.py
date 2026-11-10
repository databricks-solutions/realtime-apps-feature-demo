# Databricks notebook source
# MAGIC %md
# MAGIC # Realtime demo — bootstrap
# MAGIC
# MAGIC **Persona:** a grid operations engineer needs a live view of synthetic
# MAGIC feeder telemetry so changing conditions can be investigated quickly.
# MAGIC This bootstrap task prepares the operational serving layer for that view.
# MAGIC
# MAGIC Idempotent one-shot task that prepares Lakebase for the `stream` job:
# MAGIC resolves the Lakebase endpoint, mints an admin Postgres credential,
# MAGIC ensures the `events` table + index exist with autovacuum tuning, looks
# MAGIC up the app's service principal and grants it SELECT/DELETE, and writes
# MAGIC the `lakebase_endpoint` secret the stream reads. (The app authenticates
# MAGIC via OAuth, not a password — see resources/realtime_demo.app.yml.)

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet "pg8000==1.31.6"

# COMMAND ----------

# MAGIC %md ## 1. Configuration via widgets
# MAGIC
# MAGIC Defaults stand in when this notebook is run interactively; the job
# MAGIC passes real values via `base_parameters` in `resources/realtime_demo_init.job.yml`.

# COMMAND ----------

DEFAULTS = {
    "project_id": "realtime-demo",
    "branch_name": "production",
    "database_name": "databricks_postgres",
    "secret_scope": "realtime-demo",
    # The app's resource name. init looks up its service principal via the SDK
    # and grants that role on `events` — so the app only needs to already exist
    # (it's deployed before this one-shot runs). Empty = skip the grant.
    "app_name": "realtime-demo",
}
for k, v in DEFAULTS.items():
    dbutils.widgets.text(k, v)
p = {k: dbutils.widgets.get(k) for k in DEFAULTS}

import re

# These values are interpolated into REST paths and SQL identifiers, so accept
# only simple identifiers. `app_name` may be empty (skip the grant).
for field in ("project_id", "branch_name", "database_name", "secret_scope", "app_name"):
    if field == "app_name" and not p[field]:
        continue
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,62}", p[field]):
        raise ValueError(f"Invalid {field}: expected a simple identifier, got {p[field]!r}")

# COMMAND ----------

# MAGIC %md ## 2. Workspace client + REST helper
# MAGIC
# MAGIC The DBR-shipped `databricks-sdk` predates the Lakebase Postgres API
# MAGIC (`w.postgres.…`), so we call the REST endpoints directly. `post()`
# MAGIC is the one-liner we reuse for the JWT mint and the secret writes.

# COMMAND ----------

import requests
from databricks.sdk import WorkspaceClient
from pg8000 import dbapi

w = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}

def post(path, body):
    r = requests.post(f"{workspace_url}{path}", headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json() if r.content else None

# COMMAND ----------

# MAGIC %md ## 3. Resolve the Lakebase endpoint
# MAGIC
# MAGIC The endpoints API returns `{"endpoints": [...]}`, though some clients
# MAGIC (e.g. the CLI) unwrap to a bare list — accept both shapes.

# COMMAND ----------

branch_path = f"projects/{p['project_id']}/branches/{p['branch_name']}"
resp = requests.get(f"{workspace_url}/api/2.0/postgres/{branch_path}/endpoints",
                    headers=headers, timeout=30)
resp.raise_for_status()
payload = resp.json()
endpoints = payload if isinstance(payload, list) else payload.get("endpoints", [])
if not endpoints:
    raise RuntimeError(f"No Postgres endpoints under {branch_path}; project still provisioning?")
endpoint = endpoints[0]
lakebase_host = endpoint["status"]["hosts"]["host"]
endpoint_name = endpoint["name"]
print(f"Endpoint: {endpoint_name}")
print(f"Host:     {lakebase_host}")

# COMMAND ----------

# MAGIC %md ## 4. Mint admin Postgres credential
# MAGIC
# MAGIC Lakebase rejects the notebook session token; only a short-lived OAuth
# MAGIC JWT minted via this endpoint is accepted as the Postgres password for
# MAGIC the workspace user. We use it once below to run admin DDL.

# COMMAND ----------

admin_pg_token = post("/api/2.0/postgres/credentials", {"endpoint": endpoint_name})["token"]

# COMMAND ----------

# MAGIC %md ## 5. Ensure the table, index, autovacuum, and app grant
# MAGIC
# MAGIC Idempotent: `IF NOT EXISTS` for the table and index, and `SELECT,DELETE`
# MAGIC grants to the app's service-principal role (looked up from `app_name` via
# MAGIC the SDK). No password role is created — the app authenticates via OAuth.

# COMMAND ----------

db       = p["database_name"]
app_name = p["app_name"]

# Resolve the app's service-principal Postgres role so we can grant it. The app
# is deployed before this one-shot runs, so a plain SDK lookup works — no SP id
# to pass in, and no second deploy. It SELECTs for the API and DELETEs for the
# retention loop; INSERTs come from the stream's workspace identity.
app_sp = ""
if app_name:
    try:
        app_sp = w.apps.get(name=app_name).service_principal_client_id
    except Exception as e:  # noqa: BLE001 — grant is best-effort
        print(f"Could not resolve service principal for app '{app_name}': {e}")
# The client id is inlined as a quoted SQL identifier, so require a UUID.
if app_sp and not re.fullmatch(r"[0-9a-fA-F-]{36}", app_sp):
    raise ValueError(f"Unexpected service principal client id: {app_sp!r}")

with dbapi.connect(
    host=lakebase_host, port=5432, database=db,
    user=w.current_user.me().user_name, password=admin_pg_token,
    ssl_context=True,
) as conn:
    conn.autocommit = True
    cur = conn.cursor()
    # No bind parameters, so pg8000 sends this as one simple-protocol query,
    # which allows multiple statements.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id        BIGINT PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            metric    DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS events_ts_idx ON events (timestamp DESC);
        -- Aggressive per-table autovacuum: the 24x7 stream appends while the app
        -- deletes old rows every ~30s, so dead tuples pile up fast. Postgres
        -- defaults wait for 20% churn before vacuuming, far too slow here;
        -- without this the table bloats and both the sink and reads slow down.
        -- Guidance from the databricks-spark-structured-streaming skill.
        ALTER TABLE events SET (
            autovacuum_vacuum_scale_factor = 0,
            autovacuum_vacuum_threshold    = 200,
            autovacuum_vacuum_cost_delay   = 0
        );
    """)
    if app_sp:
        cur.execute(f'GRANT USAGE ON SCHEMA public TO "{app_sp}"')
        cur.execute(f'GRANT SELECT, DELETE ON TABLE events TO "{app_sp}"')
        print(f"events table + index ready; autovacuum tuned; SELECT+DELETE granted to app SP {app_sp}.")
    else:
        print("events table + index ready; autovacuum tuned. App SP not resolved — "
              "skipped the app grant (deploy the app first, then re-run init).")

# COMMAND ----------

# MAGIC %md ## 6. Write the streaming endpoint secret
# MAGIC
# MAGIC The `stream` job uses the native `format("postgresql")` sink, which
# MAGIC authenticates via the query-runner's workspace identity and only needs
# MAGIC `lakebase_endpoint` in `project_id.branch_id` shorthand. The app uses no
# MAGIC password secrets — it binds the Lakebase `postgres` resource and
# MAGIC authenticates via OAuth.

# COMMAND ----------

scope = p["secret_scope"]
lakebase_endpoint = f"{p['project_id']}.{p['branch_name']}"
post("/api/2.0/secrets/put",
     {"scope": scope, "key": "lakebase_endpoint", "string_value": lakebase_endpoint})
print(f"Wrote lakebase_endpoint to scope `{scope}`.")
