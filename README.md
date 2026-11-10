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

Success means **telemetry ingest → operator view remains below 500 ms**. In a representative AWS test workspace, the default settings measured approximately 50–90 ms in the KPI tile, with a pipeline floor (newest row, `server_ts − event.timestamp`) of about 15–30 ms. Treat those figures as illustrative, not a performance guarantee; cloud, region, workspace, and workload affect results.

The pipeline runs **24×7**: the stream is a continuous job that restarts itself on failure, and the app keeps the Lakebase table bounded to the trailing few minutes.

## Why Databricks

- Real-Time Mode processes telemetry with sub-second latency.
- The native Lakebase sink provides managed authentication, batching, retry, and idempotent upserts.
- Lakebase serves operational reads to a Databricks App without adding an external database platform; the App authenticates with short-lived OAuth credentials for its own service principal, so no database password is stored anywhere.
- Unity Catalog governs persistent checkpoints, supporting data assets, and the App's OpenTelemetry tables.
- Declarative Automation Bundles make the environment repeatable and reviewable.

## Layout

```
realtime-app-demo/
├── databricks.yml                      # Bundle root: variables (incl. the job-cluster spec), targets
├── resources/
│   ├── realtime_demo.lakebase.yml      # postgres_projects + checkpoint volume
│   ├── realtime_demo_init.job.yml      # one-shot setup: table, index, autovacuum, grant
│   ├── realtime_demo_stream.job.yml    # always-on 24x7 RTM stream job
│   └── realtime_demo.app.yml           # Databricks App + Lakebase (OAuth) binding + telemetry
├── scripts/deploy.sh                   # Supported end-to-end deployment
├── tests/test_app.py                   # App unit tests (run in CI)
└── src/
    ├── notebooks/
    │   ├── init.py                     # Bootstrap: table, index, autovacuum, app grant, endpoint secret
    │   └── stream.py                   # Rate → RTM → Lakebase native sink
    └── app/                            # FastAPI + static HTML/canvas UI
        ├── main.py
        ├── app.yaml
        ├── requirements.txt
        └── static/index.html
```

## Workspace prerequisites

The target workspace must satisfy all of the following:

- **Unity Catalog enabled.** The `dev` target creates a schema + volume; `prod` references a schema that already exists (see step 5).
- **Lakebase Autoscaling available.** Check via `databricks postgres list-projects` or the workspace UI's Compute → Lakebase panel.
- **Databricks Apps enabled.**
- **You have permission to** create jobs (with job compute), UC schemas/volumes, and secret scopes.
- **Databricks CLI ≥ 1.20.0** — the Lakebase `postgres` app binding uses the Autoscaling `branch`/`database` form added in 1.20.0. Check with `databricks version`.

Cost note: this demo is **always on**. Lakebase Autoscaling with `no_suspension: true` is billed continuously (the smallest setting, 0.5–1 CU, is about $0.50/h). The stream's job cluster (an `m5d.xlarge` driver + one `m5d.large` worker on AWS) also runs 24×7 at jobs-compute rates, because the stream is continuous — there is no idle autotermination. Tear down when you're done — see "Tear down" below.

### Classic-compute exception

This asset intentionally does **not** run the streaming task on Serverless. The native Lakebase Structured Streaming sink is Public Preview and requires DBR 18.3+ on classic compute; Serverless and Lakeflow Spark Declarative Pipelines are not supported for this sink. The Databricks App and Lakebase remain managed/serverless services. Reassess this exception whenever the sink's support matrix changes, and archive or update the repository if it is no longer current.

## Configure for your workspace

The bundle is workspace-agnostic. The supported deployment path creates the secret scope if needed, validates the bundle strictly, deploys everything, runs the one-shot setup job, and starts the App. The continuous stream starts on its own:

```bash
./scripts/deploy.sh <your-profile-name> [catalog] [dev|prod]
```

Requirements: Databricks CLI and `jq`. The script always requires an explicit CLI profile. Target defaults to `dev`; `prod` additionally needs step 5 below.

The manual steps below explain what the script does and support troubleshooting.

### 1. CLI profile

Make sure your Databricks CLI has a profile configured for the target workspace. If you don't have one yet:

```bash
databricks auth login --profile <your-profile-name>
```

You'll pass `--profile <your-profile-name>` to every `bundle …` command below (or export `DATABRICKS_CONFIG_PROFILE=<name>` once per shell). The targets intentionally have no host/profile baked in so the same bundle can target any workspace.

### 2. Pick a Unity Catalog

Default is `main` (works on most workspaces). Override by editing the `catalog` (and `schema`) variable defaults in `databricks.yml`, or via `export BUNDLE_VAR_catalog=<your_catalog>` at deploy time. The `dev` target *creates* the schema in this catalog (you need `CREATE SCHEMA`); the `prod` target expects it to already exist (step 5).

### 3. (Azure / GCP only) Adjust the cluster spec

Both jobs run on job compute defined by the `job_cluster` variable in `databricks.yml` — no manual step on AWS. For non-AWS workspaces, edit that variable's `default`:

- **Azure**: replace `aws_attributes` with `azure_attributes: { availability: ON_DEMAND_AZURE }` and change `node_type_id`/`driver_node_type_id` to e.g. `Standard_D4ds_v5` (worker) / `Standard_D8ds_v5` (driver).
- **GCP**: replace `aws_attributes` with `gcp_attributes: { use_preemptible_executors: false }` and use e.g. `n2-standard-4` (worker) / `n2-standard-8` (driver).

Keep the driver at ~16 GB of memory — see "Why these choices".

### 4. Create the secret scope

The `init` job writes the `lakebase_endpoint` secret here and the stream reads it. Create the scope (there are no keys to pre-seed):

```bash
databricks secrets create-scope realtime-demo
```

Override `realtime-demo` if you renamed the `secret_scope` variable.

### 5. (prod only) Pre-create the schema and grant the app

`prod` references a UC schema that must already exist, because a governed catalog won't let the deployer `CREATE SCHEMA`. Create it once (you also need `CREATE VOLUME` on it so the bundle can add the checkpoint volume):

```bash
databricks schemas create <schema> <catalog>
```

The app service principal needs **two** kinds of access — don't conflate them:

- **Postgres `events` access** (required for the app to serve data). Handled automatically — `init` looks up the app's service principal via the SDK and runs `GRANT SELECT, DELETE`. Nothing to do by hand.
- **Unity Catalog OpenTelemetry tables** (telemetry only). The service principal writes `otel_spans/metrics/logs` into this schema. If those tables stay empty after traffic, grant it as the catalog/schema owner (find the client id with `databricks apps get realtime-demo` → `service_principal_client_id`):
  ```sql
  GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
  GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
    ON SCHEMA <catalog>.<schema> TO `<app-sp-client-id>`;
  ```

## Deploy

For brevity, set this once per shell:

```bash
export DATABRICKS_CONFIG_PROFILE=<your-profile-name>
# (Optional) Only if your catalog isn't `main`:
# export BUNDLE_VAR_catalog=<your_catalog>
```

Then (use `--target prod` for the long-running deployment):

```bash
databricks bundle validate --strict --target dev

# 1. Deploy ALL resources: the Lakebase project (with its production branch,
#    databricks-postgres database, and primary endpoint), the checkpoint volume,
#    both jobs (each on its own job cluster), and the app. The continuous stream
#    job starts immediately and retries until step 2 creates the events table.
databricks bundle deploy --target dev

# 2. One-time setup (idempotent): create the events table + index + autovacuum,
#    look up the app's service principal and GRANT it SELECT, DELETE on events
#    (SELECT for the API, DELETE for retention), and write the lakebase_endpoint
#    secret the stream reads.
databricks bundle run rt_demo_init_job --target dev

# 3. Deploy the app's source code and start it. It mints an OAuth credential and
#    begins serving rows.
databricks bundle run rt_demo_app --target dev
```

The last command prints the App URL — open it; within a second or two the KPI tile shows live ingest → operator-view latency. There is no stream job to trigger manually — it's `continuous`.

> **`dev` vs `prod`.** `--target prod` is the long-running deployment (production mode, fixed names). `--target dev` is for iteration against a catalog you own — development mode prefixes job names and bundle-creates its own schema. Both targets use the same App name and Lakebase project, so don't deploy both into one workspace without overriding `project_id` and the App name.

**Redeploying after a teardown?** `bundle destroy` *soft-deletes* the Lakebase project, so a fresh deploy fails with *"a soft-deleted project … already has this resource name."* Purge it first, then redeploy:

```bash
databricks postgres delete-project projects/realtime-demo --purge
```

## Operate

The stream runs **continuously** (`run_duration_seconds=0`, `continuous: UNPAUSED`) and restarts itself after a failure. Each restart provisions a fresh job cluster (several minutes) and wipes the checkpoint, so event ids restart at 0; the UI detects this and resets its chart. `rt_demo_init_job` only needs to re-run if you recreate the table, grants, or endpoint secret. To pause, resume, or restart the stream, use **Jobs ▸ `realtime-demo-stream`**; a `bundle deploy` always re-creates it UNPAUSED.

### Tune throughput

The knobs live in `resources/realtime_demo_stream.job.yml` → the stream task's `notebook_task.base_parameters`:

- `rows_per_second` — 500 by default; 100–1000 still satisfies sub-second lag. If you raise it a lot, raise the frontend's `LIMIT` too, so each 100 ms poll still covers all new rows.
- `run_duration_seconds` — `"0"` (stream indefinitely, 24×7) by default. A positive value time-boxes a single run, then stops cleanly — use it only for short demos.
- `sink_batchinterval` — max buffer hold time for the native sink (default `"20 milliseconds"`). Must be a Spark interval string with long-form unit names (e.g. `"20 milliseconds"`, `"1 second"`) — `"20"` fails with `INVALID_INTERVAL_FORMAT.MISSING_UNIT` and `"20 ms"` fails with `INVALID_INTERVAL_FORMAT.INVALID_UNIT`. Lower = less per-row latency; higher = fewer/larger commits. The sink also flushes at `batchsize` rows (default 1000), whichever hits first.

The App's retention window lives in `src/app/app.yaml`: `RETENTION_MINUTES` (default 5) and `RETENTION_INTERVAL_SECONDS` (default 30). Keep the window small — the UI only shows the trailing 20 s.

Edit and `databricks bundle deploy` again; the continuous stream restarts onto the new values.

## Verify

1. **Bundle deploy succeeds.**
2. **Lakebase project provisioned** — Workspace UI ▸ Compute ▸ Lakebase ▸ `realtime-demo` shows `Active`.
3. **Jobs are healthy** — `realtime-demo-init` latest run SUCCESS, and `realtime-demo-stream` RUNNING (continuous — it stays running).
4. **Rows are landing and the table stays bounded** — query Lakebase from your shell (mints a short-lived credential as your workspace identity):
   ```bash
   databricks psql --project realtime-demo -- -c \
     "SELECT count(*), max(timestamp) FROM events;"
   ```
   The count stays roughly flat (~`RETENTION_MINUTES × 60 × rows_per_second`) while `max(timestamp)` tracks now.
5. **App is reachable** — Apps UI ▸ `realtime-demo` is `Running`; the URL serves the page.
6. **KPI under 500 ms** — "Telemetry ingest → operator view" stays sub-half-second while the stream is up.
7. **Telemetry lands** — after some traffic:
   ```sql
   SELECT count(*) FROM <catalog>.<schema>.otel_spans WHERE service_name = 'realtime-demo';
   ```

## Tear down

Destroy the target you deployed:

```bash
databricks bundle destroy --target dev
```

Removes both jobs, the app, the volume, and the Lakebase project (and, for `dev`, the schema; the `prod` schema is out-of-band, so it stays). **Two manual steps** still needed:

```bash
# 1. bundle destroy SOFT-deletes the Lakebase project — purge it, or a later
#    redeploy fails with "a soft-deleted project ... already has this name".
databricks postgres delete-project projects/realtime-demo --purge

# 2. The secret scope is created out-of-band, so bundle destroy doesn't remove it.
databricks secrets delete-scope realtime-demo
```

## Why these choices

| Choice | Why |
|---|---|
| Rate source | Synthetic; no external dependency; the `timestamp` field gives us a clean latency reference. |
| Lakebase Autoscaling | Postgres-compatible, low-latency reads, scales to ~1 CU for the demo. |
| Two jobs: one-shot init + continuous stream | A `continuous` job can't contain inter-task `depends_on`, so setup (`rt_demo_init_job`) is a one-shot you run once and the stream (`rt_demo_stream_job`) is its own always-on job (`run_duration_seconds=0`, `pause_status: UNPAUSED`, no job timeout, self-restarts on failure). |
| DBR 18.3 Classic, Photon off, autoscale off | Real-Time Mode requirements + the native `format("postgresql")` sink needs DBR 18.3+. |
| Job compute (shared `job_cluster` spec), not all-purpose | Each job runs on its own job cluster. The continuous stream's cluster lives for the whole run — lower DBU rate than all-purpose and fully dedicated. Trade-off: after a failure the job provisions a new cluster (several minutes) instead of reusing a warm one. |
| 16 GB driver (`m5d.xlarge`), 8 GB worker | On a 24×7 run the driver's memory grows steadily. With an 8 GB driver it was swapping from the start and the stream failed every ~2.5 days; 16 GB removes the early swapping and extends the time between restarts. The worker's memory stays low. |
| Native `format("postgresql")` streaming sink | Workspace-managed auth (no password on the streaming path), built-in batching, automatic retries on transient JDBC errors, upserts via the table's `PRIMARY KEY`. See the [Lakebase sink page](https://docs.databricks.com/aws/en/structured-streaming/lakebase). |
| `batchinterval` 20 ms | At 500 rps on one partition the flush timer dominates per-row latency; 20 ms lowers it well within Lakebase's connection and 1-CU headroom. |
| OAuth via the app service principal | The App binds the Lakebase `postgres` resource and mints short-lived OAuth credentials for its own service-principal role, refreshed in the background — no stored password. The binding injects `PG*` connection settings but not the endpoint path, so `LAKEBASE_ENDPOINT` is set in `app.yaml`. |
| Pre-pinged connection pool | Each checkout runs `SELECT 1`; a connection dropped by an endpoint bounce or Lakebase's ~24 h lifetime is replaced instead of failing the request. |
| App-side retention + aggressive per-table autovacuum | The rate source's `id` only increases, so every streamed row is an INSERT. The App deletes rows older than 5 min every ~30 s to keep the table flat; the autovacuum tuning reclaims those dead tuples quickly. The two go together. |
| OpenTelemetry to Unity Catalog | `opentelemetry-instrument` auto-instruments FastAPI; `telemetry_export_destinations` makes the platform inject the OTLP collector, so spans, metrics, and logs land in UC tables. |
| Plain HTML + canvas (no React/Recharts) | One file, no build step. The chart redraws every 100 ms (the poll cadence) into 200 ms buckets, so it adds no visible lag. |
| Trigger `realTime="5 minutes"` | "5 minutes" is the long-running batch duration (sometimes called the checkpoint interval), not how often the trigger fires. RTM runs continuously between checkpoints. |

## Common issues

| Issue | Fix |
|---|---|
| `databricks bundle validate` fails with "profile not found" | Run `databricks auth login --profile <your-profile>` and retry. |
| `init` can't write / stream can't read `lakebase_endpoint` | The secret scope doesn't exist — create it (step 4). |
| Init fails: `Provided authentication token is not a valid JWT encoding` connecting to Lakebase | The notebook session token is NOT a valid Lakebase Postgres credential. The notebook mints a short-lived JWT via `POST /api/2.0/postgres/credentials` and uses it as the admin password. |
| Init fails: `database "<x>" does not exist` | The default Lakebase Autoscaling database is `databricks_postgres` (underscore). Don't override `database_name` unless you've also created that database in Lakebase. |
| Stream task: `Data source postgresql does not support streamed writing` | DBR is < 18.3 — the native `format("postgresql")` streaming sink isn't registered. Bump `spark_version` in the `job_cluster` variable in `databricks.yml` to `18.3.x-scala2.13` or later. |
| Stream task: `JDBC_STREAMING_SINK_INVALID_OPTIONS` | Typo in an option name passed to the native sink. Recognised options: `endpoint`, `dbtable`, `database`, `upsertkey`, `batchsize`, `batchinterval`, `checkpointLocation`. |
| Stream is RUNNING but rows have stale timestamps for >60 s | The rate source persists `startTimeMs` in its checkpoint. The notebook clears the checkpoint dir at the top of the stream task; if you change that path, ensure the wipe still runs. |
| App KPI tile says `stream stopped` | The freshest event is >5 s old — the continuous stream failed or is restarting. Check `realtime-demo-stream` in Jobs; it restarts itself. |
| App page shows "Telemetry is temporarily unavailable" | The App can't read `events`. Check `databricks apps logs realtime-demo`: it needs the Lakebase `postgres` binding (deployed) and the `init` grant — re-run `rt_demo_init_job` (it looks up the App's service principal and grants `SELECT, DELETE`). |
| `otel_*` tables stay empty | The App's service principal lacks write access to the schema — see step 5. |
| Lag display is > 1 s | Check the cluster: DBR ≥ 18.3 (native sink requires it), Photon off, autoscale off, ≥ 1 worker, `realTimeMode.enabled=true`, `sortBeforeRepartition=false`, trigger is `realTime="5 minutes"` (not `processingTime`). |

## Dependencies and licenses

Only permissive dependencies allowed by the public-content policy are used directly.

| Library | Purpose | License | Source |
|---|---|---|---|
| FastAPI | App API | MIT | https://github.com/fastapi/fastapi |
| Uvicorn | ASGI server | BSD-3-Clause | https://github.com/encode/uvicorn |
| asyncpg | Async PostgreSQL client for the App | Apache-2.0 | https://github.com/MagicStack/asyncpg |
| pg8000 | PostgreSQL client for bootstrap notebook | BSD-3-Clause | https://github.com/tlocke/pg8000 |
| Databricks SDK for Python | Workspace identity, Lakebase OAuth credentials, and API client | Apache-2.0 | https://github.com/databricks/databricks-sdk-py |
| Requests | HTTPS calls from the bootstrap notebook | Apache-2.0 | https://github.com/psf/requests |
| OpenTelemetry Python (`opentelemetry-distro`, `opentelemetry-exporter-otlp-proto-grpc`) | App telemetry export | Apache-2.0 | https://github.com/open-telemetry/opentelemetry-python |
| OpenTelemetry Python Contrib (`opentelemetry-instrumentation-fastapi`) | FastAPI auto-instrumentation | Apache-2.0 | https://github.com/open-telemetry/opentelemetry-python-contrib |

Transitive dependencies are checked for known vulnerabilities in CI with `pip-audit`. Any dependency change must update this table and receive license review before merge.

## Maintenance and contribution

- Changes must arrive through a pull request; `main` is protected and requires review. The `quality` workflow must also pass once GitHub Actions is enabled for this repository by the organization.
- A Databricks L7+ reviewer must participate in the initial publication review. Maintainers must record the approved reviewer in the PR.
- Validate the demo against a currently supported DBR and its documented classic-compute exception at least every six months.
- External contributions are not accepted directly. Open an issue so a Databricks maintainer can evaluate and reproduce the change.
