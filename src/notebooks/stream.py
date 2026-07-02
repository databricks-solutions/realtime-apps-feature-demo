# Databricks notebook source
# MAGIC %md
# MAGIC # Realtime demo — stream
# MAGIC
# MAGIC Rate source → Real-Time Mode → Lakebase via the native
# MAGIC `format("postgresql")` sink (Public Preview, DBR 18.3+). Runs for
# MAGIC `run_duration_seconds` (default 1 h) then stops cleanly. Depends on
# MAGIC the `init` task to have prepared the endpoint, role, and secrets.
# MAGIC
# MAGIC Reference: `lakebase-sink-python.md` in the
# MAGIC [databricks-agent-skills](https://github.com/databricks/databricks-agent-skills)
# MAGIC `databricks-spark-structured-streaming` skill.

# COMMAND ----------

# MAGIC %md ## 1. Configuration via widgets

# COMMAND ----------

dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("secret_scope", "realtime-demo")
dbutils.widgets.text("rows_per_second", "500")
dbutils.widgets.text("run_duration_seconds", "3600")
# `batchinterval` is the native sink's max buffer hold time before
# flushing. Lower = lower per-row latency; higher = fewer/larger commits.
# Default is 100 milliseconds per docs. The sink also flushes when
# `batchsize` rows accumulate (default 1000), whichever comes first.
#
# Format: Spark interval string with long-form unit names —
# `"100 milliseconds"`, `"1 second"`, etc. NOT `"100"` (fails with
# INVALID_INTERVAL_FORMAT.MISSING_UNIT) and NOT `"100 ms"` (fails with
# INVALID_INTERVAL_FORMAT.INVALID_UNIT — the parser doesn't accept the
# `ms` abbreviation, only `milliseconds` / `millisecond` / `millis`).
dbutils.widgets.text("sink_batchinterval", "100 milliseconds")

checkpoint_path    = dbutils.widgets.get("checkpoint_path")
scope              = dbutils.widgets.get("secret_scope")
rows_per_second    = int(dbutils.widgets.get("rows_per_second"))
duration           = int(dbutils.widgets.get("run_duration_seconds"))
sink_batchinterval = dbutils.widgets.get("sink_batchinterval")

# COMMAND ----------

# MAGIC %md ## 2. Clean up state from any prior run
# MAGIC
# MAGIC With `existing_cluster_id` the Spark session is shared across job
# MAGIC runs; a cancelled run can leave its stream(s) active and holding the
# MAGIC checkpoint dir. The rate source also persists `startTimeMs` in the
# MAGIC checkpoint, so resuming from a stale one emits events with old
# MAGIC timestamps. Stop orphan streams and wipe the checkpoint so this run
# MAGIC starts fresh in clock time.

# COMMAND ----------

spark.conf.set("spark.sql.execution.sortBeforeRepartition", "false")

for s in spark.streams.active:
    print(f"Stopping orphan stream {s.id}")
    s.stop()

try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print(f"Cleared checkpoint at {checkpoint_path}")
except Exception as e:
    print(f"Checkpoint clear skipped: {e}")

# COMMAND ----------

# MAGIC %md ## 3. Start the Real-Time Mode stream
# MAGIC
# MAGIC `.trigger(realTime="5 minutes")` enables Real-Time Mode. The
# MAGIC `"5 minutes"` is the long-running batch duration (sometimes called
# MAGIC the checkpoint interval) — RTM runs continuously between checkpoints.
# MAGIC
# MAGIC The native `format("postgresql")` sink handles connection management,
# MAGIC batching, and automatic retries on transient JDBC errors. Auth is
# MAGIC workspace-managed via the query runner's identity — no password
# MAGIC needed on this path. `endpoint` is written by the `init` task to
# MAGIC `secret_scope/lakebase_endpoint` in `project_id.branch_id` shorthand.

# COMMAND ----------

from pyspark.sql.functions import col, rand

endpoint = dbutils.secrets.get(scope, "lakebase_endpoint")

stream = (
    spark.readStream
        .format("rate")
        .option("numPartitions", 1)
        .option("rowsPerSecond", rows_per_second)
        .load()
        .select(col("value").alias("id"), col("timestamp"), rand().alias("metric"))
        .writeStream
        .format("postgresql")
        .option("endpoint", endpoint)
        .option("dbtable", "events")
        .option("batchinterval", sink_batchinterval)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(realTime="5 minutes")
        .start()
)

# COMMAND ----------

# MAGIC %md ## 4. Run for the bounded duration, then stop cleanly
# MAGIC
# MAGIC `stream.stop()` flushes any pending sink buffer before exit.

# COMMAND ----------

stream.awaitTermination(timeout=duration)
if stream.isActive:
    stream.stop()
print(f"Stream stopped after {duration}s.")
