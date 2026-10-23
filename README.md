# Grid Operations Realtime App

## What the solution does

End-to-end flow:

```
rate source ──▶ Structured Streaming (Real-Time Mode) ──▶ Lakebase Postgres
                                                                  │
                                                                  ▼
                                                       FastAPI (Databricks App)
                                                                  │
                                                                  ▼
                                                       Browser polling at 100 ms
```

Success means **telemetry ingest → operator view remains below 500 ms**. In a representative AWS test workspace, the default settings measured approximately 130–200 ms. Treat those figures as illustrative, not a performance guarantee; cloud, region, workspace, and workload affect results.

## Why Databricks

- Real-Time Mode processes telemetry with sub-second latency.
- The native Lakebase sink provides managed authentication, batching, retry, and idempotent upserts.
- Lakebase serves operational reads to a Databricks App without adding an external database platform.
- Unity Catalog governs persistent checkpoints and supporting data assets.
- Declarative Automation Bundles make the environment repeatable and reviewable.

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
    ├── notebooks/
    │   ├── init.py                # Bootstrap: role, table, secrets, autovacuum
    │   └── stream.py              # Rate → RTM → Lakebase native sink
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

### Classic-compute exception

This asset intentionally does **not** run the streaming task on Serverless. The native Lakebase Structured Streaming sink is Public Preview and requires DBR 18.3+ on classic compute; Serverless and Lakeflow Spark Declarative Pipelines are not supported for this sink. The Databricks App and Lakebase remain managed/serverless services. Reassess this exception whenever the sink's support matrix changes, and archive or update the repository if it is no longer current.

## Configure for your workspace

The bundle is workspace-agnostic. The supported deployment path generates a unique password, seeds the required secret bindings, validates the bundle strictly, runs the short setup job, deploys the App, and starts the bounded realtime job without waiting for its one-hour run to finish:

```bash
./scripts/deploy.sh <your-profile-name> [catalog]
```

Requirements: Databricks CLI, `jq`, and `openssl`. The script always requires an explicit CLI profile and never stores the generated password in Git or prints it to the terminal.

The manual steps below explain what the script does and support troubleshooting.

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

### 4. Pre-seed the secret scope (manual deployment only)

The App resource validates that the four secret-binding keys exist at deploy time; the `init` notebook later rotates them to real values on first run.

```bash
databricks secrets create-scope realtime-demo
for k in lakebase_host lakebase_db lakebase_user lakebase_password; do
  echo "placeholder" | databricks secrets put-secret realtime-demo "$k"
done
```

Override `realtime-demo` if you renamed the `secret_scope` variable.

### 5. Generate a Postgres password (required for manual deployment)

```bash
export BUNDLE_VAR_pg_password=$(openssl rand -hex 24)
```

The bundle has no password default. Bundle validation fails if it is omitted, and the bootstrap task fails before making changes if it is shorter than 24 characters.

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
databricks bundle validate --strict --target dev
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

- `rows_per_second` — 500 by default; 100–1000 still satisfies sub-second lag.
- `run_duration_seconds` — 3600 (1 h) by default. Set lower for short demos, higher for soaks.
- `sink_batchinterval` — max buffer hold time for the native sink (default `"100 milliseconds"`). Must be a Spark interval string with long-form unit names (e.g. `"100 milliseconds"`, `"1 second"`) — `"100"` fails with `INVALID_INTERVAL_FORMAT.MISSING_UNIT` and `"100 ms"` fails with `INVALID_INTERVAL_FORMAT.INVALID_UNIT`. Lower = less per-row latency; higher = fewer/larger commits. The sink also flushes at `batchsize` rows (default 1000), whichever hits first.

Edit and redeploy; the next `bundle run rt_demo_job` picks up the new values.

## Verify

1. **Bundle deploy succeeds.**
2. **Lakebase project provisioned** — Workspace UI ▸ Compute ▸ Lakebase ▸ `realtime-demo` shows `Active`.
3. **Job run is healthy** — Workflows ▸ `realtime-demo` ▸ latest run: `init` SUCCESS, `stream` RUNNING (and will stay RUNNING for `run_duration_seconds` before terminating cleanly).
4. **Rows are landing** — from any notebook attached to the cluster:
   ```python
   from pg8000 import dbapi
   token = dbutils.secrets.get("realtime-demo", "lakebase_password")
   conn = dbapi.connect(
       host=dbutils.secrets.get("realtime-demo", "lakebase_host"),
       port=5432,
       database=dbutils.secrets.get("realtime-demo", "lakebase_db"),
       user=dbutils.secrets.get("realtime-demo", "lakebase_user"),
       password=token,
       ssl_context=True,
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
| DBR 18.3 Classic, Photon off, autoscale off | Real-Time Mode requirements + the native `format("postgresql")` sink needs DBR 18.3+. |
| Bundle-managed All-Purpose cluster, not job cluster | Skips 3–5 min cluster boot per run; cluster autoterminates after 120 min idle. |
| Native `format("postgresql")` streaming sink | Workspace-managed auth (no password on the streaming path), built-in batching, automatic retries on transient JDBC errors, upserts via the table's `PRIMARY KEY`. See the [Lakebase sink page](https://docs.databricks.com/aws/en/structured-streaming/lakebase). |
| Password auth via secret scope (app only) | Databricks Apps runs in a separate security context and can't inherit workspace OAuth — the FastAPI app uses password auth with a Lakebase role. The streaming path uses workspace-managed OAuth via the native sink. |
| Aggressive per-table autovacuum on `events` | Across successive runs the rate source resets `startTimeMs` and re-emits the same `id`s, turning INSERTs into hot-key UPDATEs. Autovacuum defaults are far too slow for this pattern — see the `databricks-spark-structured-streaming` skill's Lakebase-sink guidance. |
| Plain HTML + canvas (no React/Recharts) | One file, no build step. |
| Trigger `realTime="5 minutes"` | "5 minutes" is the long-running batch duration (sometimes called the checkpoint interval), not how often the trigger fires. RTM runs continuously between checkpoints. |

## Common issues

| Issue | Fix |
|---|---|
| `databricks bundle validate` fails with "profile not found" | Run `databricks auth login --profile <your-profile>` and retry. |
| Bundle deploy fails: `Invalid secret resource …: Secret with scope realtime-demo and key … does not exist` | Run step 4 of "Configure for your workspace" (the secret-scope seeding). |
| Init fails: `Provided authentication token is not a valid JWT encoding` connecting to Lakebase | The notebook session token is NOT a valid Lakebase Postgres credential. The notebook mints a short-lived JWT via `POST /api/2.0/postgres/credentials` and uses it as the admin password. |
| Init fails: `database "<x>" does not exist` | The default Lakebase Autoscaling database is `databricks_postgres` (underscore). Don't override `database_name` unless you've also created that database in Lakebase. |
| Init fails: `syntax error at or near "$1"` on `CREATE ROLE` | Postgres DDL doesn't accept bind parameters. The notebook inlines the password literal with escaping; don't replace with parameterised execute. |
| Stream task: `Data source postgresql does not support streamed writing` | The workspace's DBR is < 18.3 — the native `format("postgresql")` streaming sink isn't registered. Bump `spark_version` in `resources/cluster.yml` to `18.3.x-scala2.13` or later. |
| Stream task: `JDBC_STREAMING_SINK_INVALID_OPTIONS` | Typo in an option name passed to the native sink. Recognised options: `endpoint`, `dbtable`, `database`, `upsertkey`, `batchsize`, `batchinterval`, `checkpointLocation`. |
| Stream is RUNNING but rows have stale timestamps for >60 s | The rate source persists `startTimeMs` in its checkpoint. The notebook clears the checkpoint dir at the top of the stream task; if you change that path, ensure the wipe still runs. |
| Stream is RUNNING but no rows in `events` for >60 s | Probably an orphan stream from a previous run holding the checkpoint lock on the shared cluster. Cancel the active job run, restart the cluster from the Compute UI (or `databricks clusters restart <id>`), and trigger a fresh `bundle run rt_demo_job`. |
| App KPI tile says `stream stopped` | The freshest event in Postgres is more than 5 s old — usually means the job's `run_duration_seconds` elapsed. Trigger another run. |
| App page is stuck on `—` | Env vars haven't picked up the rotated secrets (true after the very first bundle deploy + run). Run `databricks apps stop && databricks apps start realtime-demo`. |
| Lag display is > 1 s | Check the cluster: DBR ≥ 18.3 (native sink requires it), Photon off, autoscale off, ≥ 1 worker, `realTimeMode.enabled=true`, `sortBeforeRepartition=false`, trigger is `realTime="5 minutes"` (not `processingTime`). |

## Dependencies and licenses

Only permissive dependencies allowed by the public-content policy are used directly.

| Library | Purpose | License | Source |
|---|---|---|---|
| FastAPI | App API | MIT | https://github.com/fastapi/fastapi |
| Uvicorn | ASGI server | BSD-3-Clause | https://github.com/encode/uvicorn |
| asyncpg | Async PostgreSQL client for the App | Apache-2.0 | https://github.com/MagicStack/asyncpg |
| pg8000 | PostgreSQL client for bootstrap notebook | BSD-3-Clause | https://github.com/tlocke/pg8000 |
| Databricks SDK for Python | Workspace identity and API client | Apache-2.0 | https://github.com/databricks/databricks-sdk-py |
| Requests | HTTPS calls from the bootstrap notebook | Apache-2.0 | https://github.com/psf/requests |

Transitive dependencies are checked for known vulnerabilities in CI with `pip-audit`. Any dependency change must update this table and receive license review before merge.

## Maintenance and contribution

- Changes must arrive through a pull request; `main` is protected and requires review. The `quality` workflow must also pass once GitHub Actions is enabled for this repository by the organization.
- A Databricks L7+ reviewer must participate in the initial publication review. Maintainers must record the approved reviewer in the PR.
- Validate the demo against a currently supported DBR and its documented classic-compute exception at least every six months.
- External contributions are not accepted directly. Open an issue so a Databricks maintainer can evaluate and reproduce the change.
