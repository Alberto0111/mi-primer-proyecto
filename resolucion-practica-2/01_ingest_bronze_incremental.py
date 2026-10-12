# Databricks notebook source
# MAGIC %md
# MAGIC # 1 — Ingesta Bronze incremental
# MAGIC
# MAGIC `COPY INTO` registra qué archivos físicos ya procesó. Volver a ejecutar esta tarea no vuelve a cargar el mismo CSV. Luego se expone una vista unificada entre el Bronze inicial y los lotes nuevos.

# COMMAND ----------

dbutils.widgets.text("student_id", "alumno01")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"])
dbutils.widgets.text("expected_batch_id", "batch_002")
dbutils.widgets.text("job_run_id", "interactive")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
create_course_namespace(spark, config)
initial = qualified_table(config, "bronze_transactions")
incremental = qualified_table(config, "bronze_transactions_incremental")
unified = qualified_table(config, "bronze_transactions_all")
assert spark.catalog.tableExists(initial), "Falta bronze_transactions. Ejecutá la práctica 1."

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {incremental} (
  transaction_id STRING, customer_id STRING, product_id STRING, event_ts STRING,
  amount STRING, payment_channel STRING, device_id STRING, is_fraud STRING,
  source_batch_id STRING, updated_at STRING
) USING DELTA
""")
copy_result = spark.sql(f"""
COPY INTO {incremental}
FROM '{config.volume_path}/incoming/transactions/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'PERMISSIVE')
""")
display(copy_result)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {unified} AS
SELECT transaction_id, customer_id, product_id, event_ts, amount, payment_channel, device_id, is_fraud,
       'initial' AS source_batch_id, event_ts AS updated_at
FROM {initial}
UNION ALL
SELECT transaction_id, customer_id, product_id, event_ts, amount, payment_channel, device_id, is_fraud,
       source_batch_id, updated_at
FROM {incremental}
""")
expected = dbutils.widgets.get("expected_batch_id").strip()
if expected:
    found = spark.table(incremental).where(f"source_batch_id = '{expected}'").count()
    assert found > 0, f"No se encontró {expected}. Ejecutá antes 00_generate_new_batch."
display(spark.table(incremental).groupBy("source_batch_id").count().orderBy("source_batch_id"))