# Databricks notebook source
# MAGIC %md
# MAGIC # Realtime demo — stream
# MAGIC
# MAGIC Rate source → Real-Time Mode → Lakebase via `LakebaseSink`. Runs for
# MAGIC `run_duration_seconds` (default 1 h) then stops cleanly. Depends on
# MAGIC the `init` task to have prepared the role, table, and secret scope.

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet "psycopg[binary]>=3.1"

# COMMAND ----------

# MAGIC %md ## 1. Configuration via widgets

# COMMAND ----------

dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("secret_scope", "realtime-demo")
dbutils.widgets.text("rows_per_second", "10")
dbutils.widgets.text("run_duration_seconds", "3600")

checkpoint_path = dbutils.widgets.get("checkpoint_path")
scope           = dbutils.widgets.get("secret_scope")
rows_per_second = int(dbutils.widgets.get("rows_per_second"))
duration        = int(dbutils.widgets.get("run_duration_seconds"))

# COMMAND ----------

# MAGIC %md ## 2. Import the `LakebaseSink`
# MAGIC
# MAGIC The sink lives in `src/lakebase_sink.py` next to the notebook
# MAGIC directory. Add the workspace path of its parent to `sys.path` so the
# MAGIC notebook can import it like a regular module.

# COMMAND ----------

import os, sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.insert(0, "/Workspace" + os.path.dirname(os.path.dirname(notebook_path)))
from lakebase_sink import LakebaseSink

# COMMAND ----------

# MAGIC %md ## 3. Clean up state from any prior run
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

# MAGIC %md ## 4. Build the `LakebaseSink`
# MAGIC
# MAGIC Reads the Lakebase connection details written by the `init` task and
# MAGIC connects via Postgres password auth — executor-side custom sinks
# MAGIC can't use OAuth.

# COMMAND ----------

sink = LakebaseSink(
    host=dbutils.secrets.get(scope, "lakebase_host"),
    database=dbutils.secrets.get(scope, "lakebase_db"),
    user=dbutils.secrets.get(scope, "lakebase_user"),
    password=dbutils.secrets.get(scope, "lakebase_password"),
    table="events",
    key_col="id",
)

# COMMAND ----------

# MAGIC %md ## 5. Start the Real-Time Mode stream
# MAGIC
# MAGIC `.trigger(realTime="5 minutes")` enables Real-Time Mode; the
# MAGIC `"5 minutes"` is the *checkpoint interval*, not how often the trigger
# MAGIC fires. RTM runs continuously between checkpoints.

# COMMAND ----------

from pyspark.sql.functions import col, rand

stream = (
    spark.readStream
        .format("rate")
        .option("numPartitions", 1)
        .option("rowsPerSecond", rows_per_second)
        .load()
        .select(col("value").alias("id"), col("timestamp"), rand().alias("metric"))
        .writeStream
        .foreach(sink)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(realTime="5 minutes")
        .start()
)

# COMMAND ----------

# MAGIC %md ## 6. Run for the bounded duration, then stop cleanly
# MAGIC
# MAGIC `stream.stop()` drains the sink via its `close()` method so any
# MAGIC buffered rows commit before exit.

# COMMAND ----------

stream.awaitTermination(timeout=duration)
if stream.isActive:
    stream.stop()
print(f"Stream stopped after {duration}s.")
