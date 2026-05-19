# Realtime App Demo

End-to-end realtime demo on Databricks:

```
rate source ──▶ Structured Streaming (Real-Time Mode) ──▶ Lakebase Postgres
                                                                  │
                                                                  ▼
                                                       FastAPI (Databricks App)
                                                                  │
                                                                  ▼
                                                       Browser polling at 100 ms
```

Goal: demonstrate sub-second pipeline latency from a row being emitted by Spark to it being returned by the API. Measured on a typical AWS workspace with the default settings, **ingest → serve** lands around **130–200 ms**.

## Layout

```
realtime-app-demo/
├── databricks.yml                 # Bundle root: variables, targets
├── resources/
│   ├── lakebase.yml               # postgres_projects, schema, volume
│   ├── cluster.yml                # All-Purpose cluster (RTM-configured)
│   ├── job.yml                    # Triggered RTM job (init → stream tasks)
│   └── app.yml                    # Databricks App + secret bindings
└── src/
    ├── lakebase_sink.py           # Streaming ForeachWriter
    ├── notebooks/
    │   ├── init.py                # Bootstrap: role, table, secrets
    │   └── stream.py              # Rate → LakebaseSink stream
    └── app/                       # FastAPI + static HTML/canvas UI
        ├── main.py
        ├── app.yaml
        ├── requirements.txt
        └── static/index.html
```

## Workspace prerequisites

The target workspace must satisfy all of the following:

- **Unity Catalog enabled.** The bundle creates a schema + volume.
- **Lakebase Autoscaling available.** Check via `databricks postgres list-projects` or the workspace UI's Compute → Lakebase panel.
- **Databricks Apps enabled.**
- **You have permission to** create All-Purpose clusters, UC schemas/volumes, secret scopes, and jobs.

Cost note: Lakebase Autoscaling with `no_suspension: true` is billed continuously (the smallest setting, 0.5–1 CU, is about $0.50/h). The All-Purpose cluster the bundle creates (`m5d.large` × 2 on AWS) is about $0.30/h while running; it autoterminates after 120 min idle. Tear down when you're done — see "Tear down" below.

## Configure for your workspace

The bundle is workspace-agnostic but ships with placeholders. Make these edits before your first deploy:

### 1. CLI profile

Make sure your Databricks CLI has a profile configured for the target workspace. If you don't have one yet:

```bash
databricks auth login --profile <your-profile-name>
```

You'll pass `--profile <your-profile-name>` to every `bundle …` command below (or export `DATABRICKS_CONFIG_PROFILE=<name>` once per shell). The bundle's `dev` target intentionally has no host/profile baked in so the same bundle can target any workspace.

### 2. Pick a Unity Catalog

Default is `main` (works on most workspaces). Override if your workspace uses a different catalog name, either by editing the `catalog` variable's default in `databricks.yml`, or via `export BUNDLE_VAR_catalog=<your_catalog>` at deploy time.

### 3. (Azure / GCP only) Adjust the cluster spec

The bundle creates the All-Purpose cluster automatically — no manual step on AWS. For non-AWS workspaces, edit `resources/cluster.yml`:

- **Azure**: replace `aws_attributes` with `azure_attributes: { availability: ON_DEMAND_AZURE }` and change `node_type_id`/`driver_node_type_id` to e.g. `Standard_D4ds_v5`.
- **GCP**: replace `aws_attributes` with `gcp_attributes: { use_preemptible_executors: false }` and use e.g. `n2-standard-4`.

### 4. Pre-seed the secret scope

The App resource validates that the four secret-binding keys exist at deploy time; the `init` notebook later rotates them to real values on first run.

```bash
databricks secrets create-scope realtime-demo
for k in lakebase_host lakebase_db lakebase_user lakebase_password; do
  echo "placeholder" | databricks secrets put-secret realtime-demo "$k"
done
```

Override `realtime-demo` if you renamed the `secret_scope` variable.

### 5. (Optional) Override the demo Postgres password

The default password literal is `demo-password-please-rotate`. For a real deployment, override it:

```bash
export BUNDLE_VAR_pg_password=$(openssl rand -hex 24)
```

## Deploy

For brevity, set this once per shell:

```bash
export DATABRICKS_CONFIG_PROFILE=<your-profile-name>
# (Optional) Only if your catalog isn't `main`:
# export BUNDLE_VAR_catalog=<your_catalog>
```

Then:

```bash
cd realtime-app-demo
databricks bundle validate --target dev
databricks bundle deploy   --target dev

# Deploy the app's source code. First time only — subsequent `bundle deploy`s
# do this implicitly when source changes.
databricks bundle run rt_demo_app --target dev
```

The second `bundle run` prints the deployed App URL — open it to confirm the UI loads (it'll show `stream stopped` until a stream run is going).

## Run

```bash
databricks bundle run rt_demo_job --target dev
```

This triggers one bounded job run: `init` provisions the Postgres role/table/secrets, then `stream` writes events for `run_duration_seconds` (default 3600 = 1 hour) and exits. After the first run completes, restart the App once so it picks up the now-real (non-placeholder) credentials from its env vars:

```bash
databricks apps stop  realtime-demo
databricks apps start realtime-demo
```

To re-run for another hour, just `databricks bundle run rt_demo_job --target dev` again.

### Tune throughput or run length

Both knobs live in `resources/job.yml` → `tasks[1].notebook_task.base_parameters`:

- `rows_per_second` — 10 is the readable default; 100–1000 still satisfies sub-second lag.
- `run_duration_seconds` — 3600 (1 h) by default. Set lower for short demos, higher for soaks.

Edit and redeploy; the next `bundle run rt_demo_job` picks up the new values.

## Verify

1. **Bundle deploy succeeds.**
2. **Lakebase project provisioned** — Workspace UI ▸ Compute ▸ Lakebase ▸ `realtime-demo` shows `Active`.
3. **Job run is healthy** — Workflows ▸ `realtime-demo` ▸ latest run: `init` SUCCESS, `stream` RUNNING (and will stay RUNNING for `run_duration_seconds` before terminating cleanly).
4. **Rows are landing** — from any notebook attached to the cluster:
   ```python
   import psycopg
   token = dbutils.secrets.get("realtime-demo", "lakebase_password")
   conn = psycopg.connect(
       host=dbutils.secrets.get("realtime-demo", "lakebase_host"),
       port=5432,
       dbname=dbutils.secrets.get("realtime-demo", "lakebase_db"),
       user=dbutils.secrets.get("realtime-demo", "lakebase_user"),
       password=token,
       sslmode="require",
   )
   cur = conn.cursor()
   cur.execute("SELECT count(*), max(timestamp) FROM events")
   print(cur.fetchone())
   ```
   The count should grow on every re-run.
5. **App is reachable** — Apps UI ▸ `realtime-demo` is `Running`; the URL serves the page.
6. **KPI under 500 ms** — "Ingest → serve" stays sub-half-second while the stream is up.

## Tear down

```bash
databricks bundle destroy --target dev
```

Removes the cluster, job, app, schema, volume, and Lakebase project. **One manual step** still needed:

```bash
# The secret scope is created out-of-band, so `bundle destroy` doesn't remove it.
databricks secrets delete-scope realtime-demo
```

## Why these choices

| Choice | Why |
|---|---|
| Rate source | Synthetic; no external dependency; the `timestamp` field gives us a clean latency reference. |
| Lakebase Autoscaling | Postgres-compatible, low-latency reads, scales to ~1 CU for the demo. |
| Triggered job, two tasks (init → stream) | Stream is bounded by `awaitTermination(timeout)`; each run is one hour by default. Init runs once per run and is idempotent. |
| DBR 17.3 LTS Classic, Photon off, autoscale off | Real-Time Mode requirements. |
| Bundle-managed All-Purpose cluster, not job cluster | Skips 3–5 min cluster boot per run; cluster autoterminates after 120 min idle. |
| Password auth via secret scope (not OAuth) | OAuth doesn't survive the executor-side `ForeachWriter` lifecycle. |
| `INSERT … ON CONFLICT` upsert in the sink | Idempotent restarts — re-processing the same row no-ops. |
| Plain HTML + canvas (no React/Recharts) | One file, no build step, ~85 lines. |
| Trigger `realTime="5 minutes"` | "5 minutes" is the checkpoint interval, not how often the trigger fires. RTM runs continuously between checkpoints. |

## Common issues

| Issue | Fix |
|---|---|
| `databricks bundle validate` fails with "profile not found" | Run `databricks auth login --profile <your-profile>` and retry. |
| Bundle deploy fails: `Invalid secret resource …: Secret with scope realtime-demo and key … does not exist` | Run step 4 of "Configure for your workspace" (the secret-scope seeding). |
| Init fails: `Provided authentication token is not a valid JWT encoding` connecting to Lakebase | The notebook session token is NOT a valid Lakebase Postgres credential. The notebook mints a short-lived JWT via `POST /api/2.0/postgres/credentials` and uses it as the admin password. |
| Init fails: `database "<x>" does not exist` | The default Lakebase Autoscaling database is `databricks_postgres` (underscore). Don't override `database_name` unless you've also created that database in Lakebase. |
| Init fails: `syntax error at or near "$1"` on `CREATE ROLE` | Postgres DDL doesn't accept bind parameters. The notebook inlines the password literal with escaping; don't replace with parameterised execute. |
| Stream task: `IndentationError: unexpected indent (lakebase_sink.py, line 1)` | `bundle deploy` corrupts the first line of `lakebase_sink.py` if it begins with `"""`. Keep the file starting with a `#` comment line (current code does). |
| Stream task: `ImportError: cannot import name 'ForeachWriter' from 'pyspark.sql.streaming'` | `ForeachWriter` is a Scala symbol; Python uses duck-typed objects with `open/process/close`. `lakebase_sink.py` already does this; if you re-add the base-class import, this is why it breaks. |
| Stream is RUNNING but rows have stale timestamps for >60 s | The rate source persists `startTimeMs` in its checkpoint. The notebook clears the checkpoint dir at the top of the stream task; if you change that path, ensure the wipe still runs. |
| Stream is RUNNING but no rows in `events` for >60 s | Probably an orphan stream from a previous run holding the checkpoint lock on the shared cluster. Cancel the active job run, restart the cluster from the Compute UI (or `databricks clusters restart <id>`), and trigger a fresh `bundle run rt_demo_job`. |
| App KPI tile says `stream stopped` | The freshest event in Postgres is more than 5 s old — usually means the job's `run_duration_seconds` elapsed. Trigger another run. |
| App page is stuck on `—` | Env vars haven't picked up the rotated secrets (true after the very first bundle deploy + run). Run `databricks apps stop && databricks apps start realtime-demo`. |
| Lag display is > 1 s | Check the cluster: DBR ≥ 17.3 LTS, Photon off, autoscale off, ≥ 1 worker, `realTimeMode.enabled=true`, `sortBeforeRepartition=false`, trigger is `realTime="5 minutes"` (not `processingTime`). |
