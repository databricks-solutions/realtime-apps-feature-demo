# Databricks notebook source
# MAGIC %md
# MAGIC # Realtime demo — bootstrap
# MAGIC
# MAGIC Idempotent one-shot task that prepares Lakebase for the `stream` task:
# MAGIC resolves the Lakebase endpoint, mints an admin Postgres credential,
# MAGIC creates (or rotates) the demo role, ensures the `events` table + index
# MAGIC exist, and writes the four connection secrets to the secret scope.

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]>=3.1"

# COMMAND ----------

# MAGIC %md ## 1. Configuration via widgets
# MAGIC
# MAGIC Defaults stand in when this notebook is run interactively; the job
# MAGIC passes real values via `base_parameters` in `resources/job.yml`.

# COMMAND ----------

DEFAULTS = {
    "project_id": "realtime-demo",
    "branch_name": "production",
    "database_name": "databricks_postgres",
    "pg_role": "demo_writer",
    "pg_password": "demo-password-please-rotate",
    "secret_scope": "realtime-demo",
}
for k, v in DEFAULTS.items():
    dbutils.widgets.text(k, v)
p = {k: dbutils.widgets.get(k) for k in DEFAULTS}

# COMMAND ----------

# MAGIC %md ## 2. Workspace client + REST helper
# MAGIC
# MAGIC The DBR-shipped `databricks-sdk` predates the Lakebase Postgres API
# MAGIC (`w.postgres.…`), so we call the REST endpoints directly. `post()`
# MAGIC is the one-liner we reuse for the JWT mint and the secret writes.

# COMMAND ----------

import requests
from databricks.sdk import WorkspaceClient
import psycopg

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

# MAGIC %md ## 5. Bootstrap role, table, grants, and index
# MAGIC
# MAGIC Idempotent: `CREATE` or `ALTER` for the role (rotates the password on
# MAGIC every run), `IF NOT EXISTS` for the table and index. Postgres DDL
# MAGIC doesn't accept bind parameters, so the role password is inlined as a
# MAGIC SQL literal with single-quote escaping.

# COMMAND ----------

pw_lit = "'" + p["pg_password"].replace("'", "''") + "'"
role   = p["pg_role"]
db     = p["database_name"]

with psycopg.connect(
    host=lakebase_host, port=5432, dbname=db,
    user=w.current_user.me().user_name, password=admin_pg_token,
    sslmode="require", autocommit=True,
) as conn, conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    verb = "ALTER" if cur.fetchone() else "CREATE"
    cur.execute(f'{verb} ROLE "{role}" WITH LOGIN PASSWORD {pw_lit}')
    cur.execute(f"""
        GRANT ALL ON DATABASE "{db}" TO "{role}";
        GRANT ALL ON SCHEMA public  TO "{role}";
        CREATE TABLE IF NOT EXISTS events (
            id        BIGINT PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            metric    DOUBLE PRECISION
        );
        GRANT ALL ON TABLE events TO "{role}";
        CREATE INDEX IF NOT EXISTS events_ts_idx ON events (timestamp DESC);
    """)
print(f"Role {role} ready; events table + index ready.")

# COMMAND ----------

# MAGIC %md ## 6. Refresh connection secrets
# MAGIC
# MAGIC The `stream` task and the FastAPI app both read these four keys from
# MAGIC the secret scope to connect with Postgres password auth.

# COMMAND ----------

scope = p["secret_scope"]
for key, val in [("lakebase_host",     lakebase_host),
                 ("lakebase_db",       db),
                 ("lakebase_user",     role),
                 ("lakebase_password", p["pg_password"])]:
    post("/api/2.0/secrets/put", {"scope": scope, "key": key, "string_value": val})
print(f"Wrote 4 secrets to scope `{scope}`.")
