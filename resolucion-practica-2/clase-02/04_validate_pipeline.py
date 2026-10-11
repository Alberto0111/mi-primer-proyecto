# Databricks notebook source
# MAGIC %md
# MAGIC # 4 — Validación end-to-end
# MAGIC
# MAGIC Esta tarea convierte las expectativas en controles ejecutables. Si una condición falla, el Job queda en rojo. También registra métricas por corrida para demostrar que reprocesar el mismo lote no duplica resultados.

# COMMAND ----------

dbutils.widgets.text("student_id", "alumno01")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"])
dbutils.widgets.text("expected_batch_id", "batch_002")
dbutils.widgets.text("job_run_id", "interactive")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

from decimal import Decimal
import re
from pyspark.sql import functions as F

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
create_course_namespace(spark, config)
expected = dbutils.widgets.get("expected_batch_id").strip().lower()
assert re.fullmatch(r"batch_[0-9]{3,6}", expected), "expected_batch_id debe tener formato batch_002"
job_run_id = dbutils.widgets.get("job_run_id").strip()[:100] or "interactive"
names = {name: qualified_table(config, name) for name in [
    "bronze_transactions_incremental", "silver_transactions", "silver_transactions_quarantine",
    "silver_customers", "silver_products", "gold_daily_sales",
    "gold_customer_risk", "gold_batch_summary", "pipeline_run_audit"]}
missing = [name for name, table in names.items() if name != "pipeline_run_audit" and not spark.catalog.tableExists(table)]
assert not missing, f"Faltan salidas del pipeline: {missing}"

# COMMAND ----------

tx = spark.table(names["silver_transactions"])
quarantine = spark.table(names["silver_transactions_quarantine"])
daily = spark.table(names["gold_daily_sales"])
batch = spark.table(names["gold_batch_summary"])
silver_metrics = tx.agg(F.count("*").alias("rows"), F.countDistinct("transaction_id").alias("distinct_ids"), F.sum("amount").cast("decimal(18,2)").alias("amount")).first()
gold_metrics = daily.agg(F.sum("transaction_count").alias("rows"), F.sum("total_amount").cast("decimal(18,2)").alias("amount")).first()
batch_metrics = batch.where(F.col("source_batch_id") == expected).first()
bronze_batch_rows = spark.table(names["bronze_transactions_incremental"]).where(F.col("source_batch_id") == expected).count()
silver_batch_rows = tx.where(F.col("source_batch_id") == expected).count()
rejected_batch_rows = quarantine.where(F.col("source_batch_id") == expected).count()
correction = tx.where(F.col("transaction_id") == 42).select("amount", "source_batch_id").first()
checks = {
    "silver_no_duplicate_ids": silver_metrics.rows == silver_metrics.distinct_ids,
    "gold_reconciles_rows": gold_metrics.rows == silver_metrics.rows,
    "gold_reconciles_amount": gold_metrics.amount == silver_metrics.amount,
    "batch_arrived_in_bronze": bronze_batch_rows > 0,
    "batch_arrived_in_silver": silver_batch_rows > 0,
    "batch_has_controlled_rejections": rejected_batch_rows >= 2,
    "batch_is_visible_in_gold": batch_metrics is not None and batch_metrics.accepted_transactions == silver_batch_rows and batch_metrics.rejected_transactions == rejected_batch_rows,
    "historical_correction_applied": correction is not None and correction.amount == Decimal("1999.99") and correction.source_batch_id == expected,
}
display(spark.createDataFrame([(name, passed) for name, passed in checks.items()], ["control", "passed"]))
failed = [name for name, passed in checks.items() if not passed]
assert not failed, f"Fallaron controles: {failed}"

# COMMAND ----------

audit = names["pipeline_run_audit"]
spark.sql(f"""CREATE TABLE IF NOT EXISTS {audit} (
 job_run_id STRING, expected_batch_id STRING, silver_rows BIGINT, quarantine_rows BIGINT,
 gold_rows BIGINT, gold_total_amount DECIMAL(18,2), recorded_at TIMESTAMP, idempotence_compared BOOLEAN
) USING DELTA""")
prior = (spark.table(audit).orderBy(F.col("recorded_at").desc())
    .select("expected_batch_id", "silver_rows", "quarantine_rows", "gold_rows", "gold_total_amount").first())
comparable = prior is not None and prior.expected_batch_id == expected
current_metrics = (silver_metrics.rows, quarantine.count(), daily.count(), gold_metrics.amount)
if comparable:
    prior_metrics = (prior.silver_rows, prior.quarantine_rows, prior.gold_rows, prior.gold_total_amount)
    assert current_metrics == prior_metrics, f"La reejecución cambió métricas: antes={prior_metrics}, ahora={current_metrics}"
audit_row = spark.range(1).select(
    F.lit(job_run_id).alias("job_run_id"), F.lit(expected).alias("expected_batch_id"),
    F.lit(current_metrics[0]).cast("long").alias("silver_rows"),
    F.lit(current_metrics[1]).cast("long").alias("quarantine_rows"),
    F.lit(current_metrics[2]).cast("long").alias("gold_rows"),
    F.lit(current_metrics[3]).cast("decimal(18,2)").alias("gold_total_amount"),
    F.current_timestamp().alias("recorded_at"), F.lit(comparable).alias("idempotence_compared"))
audit_row.write.mode("append").saveAsTable(audit)
display(spark.table(audit).orderBy(F.col("recorded_at").desc()).limit(10))
print("Validación completa. Reejecución consecutiva comparada:", comparable)