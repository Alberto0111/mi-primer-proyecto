# Databricks notebook source
# MAGIC %md
# MAGIC # 2 — Construcción de Silver
# MAGIC
# MAGIC Silver tipa, deduplica y valida. Los registros aceptados se integran con `MERGE`; los rechazados quedan en cuarentena con una causa explícita. La corrección de la transacción 42 permite observar un `UPDATE` real.

# COMMAND ----------

dbutils.widgets.text("student_id", "alumno01")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"])
dbutils.widgets.text("expected_batch_id", "batch_002")
dbutils.widgets.text("job_run_id", "interactive")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/quality_rules

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
create_course_namespace(spark, config)
bronze_all = qualified_table(config, "bronze_transactions_all")
assert spark.catalog.tableExists(bronze_all), "Falta bronze_transactions_all. Ejecutá la tarea 01."
customers = qualified_table(config, "silver_customers")
products = qualified_table(config, "silver_products")
transactions = qualified_table(config, "silver_transactions")
quarantine = qualified_table(config, "silver_transactions_quarantine")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {customers} USING DELTA AS
SELECT customer_id, country, segment, created_date, email
FROM (
  SELECT try_cast(customer_id AS BIGINT) customer_id, country, segment,
         try_cast(created_date AS DATE) created_date, lower(trim(email)) email,
         row_number() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) rn
  FROM {qualified_table(config, 'bronze_customers')}
) WHERE rn = 1 AND customer_id IS NOT NULL
""")
spark.sql(f"""
CREATE OR REPLACE TABLE {products} USING DELTA AS
SELECT product_id, category, price
FROM (
  SELECT try_cast(product_id AS BIGINT) product_id, category, try_cast(price AS DECIMAL(12,2)) price,
         row_number() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) rn
  FROM {qualified_table(config, 'bronze_products')}
) WHERE rn = 1 AND product_id IS NOT NULL AND price > 0
""")

# COMMAND ----------

raw_columns = ["transaction_id", "customer_id", "product_id", "event_ts", "amount", "payment_channel", "device_id", "is_fraud", "source_batch_id", "updated_at"]
typed = add_transaction_types(spark.table(bronze_all).dropDuplicates(raw_columns))
known_customers = spark.table(customers).select(F.col("customer_id").alias("known_customer"))
known_products = spark.table(products).select(F.col("product_id").alias("known_product"))
checked = (typed
    .join(known_customers, F.col("customer_id_typed") == F.col("known_customer"), "left")
    .join(known_products, F.col("product_id_typed") == F.col("known_product"), "left"))
checked = add_quality_reason(checked)
record_key = F.coalesce(F.col("transaction_id_typed").cast("string"), F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in raw_columns]), 256))
latest = (checked.withColumn("_record_key", record_key)
    .withColumn("_rn", F.row_number().over(Window.partitionBy("_record_key").orderBy(F.col("updated_at_typed").desc_nulls_last(), F.col("source_batch_id").desc())))
    .where("_rn = 1"))

# COMMAND ----------

valid = (latest.where(F.col("quality_reason").isNull()).select(
    F.col("transaction_id_typed").alias("transaction_id"),
    F.col("customer_id_typed").alias("customer_id"),
    F.col("product_id_typed").alias("product_id"),
    F.col("event_ts_typed").alias("event_ts"),
    F.col("amount_typed").alias("amount"), "payment_channel", "device_id",
    F.col("is_fraud_typed").alias("is_fraud"), "source_batch_id",
    F.col("updated_at_typed").alias("updated_at"), F.current_timestamp().alias("processed_at")))
spark.sql(f"""CREATE TABLE IF NOT EXISTS {transactions} (
 transaction_id BIGINT, customer_id BIGINT, product_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(12,2),
 payment_channel STRING, device_id STRING, is_fraud INT, source_batch_id STRING, updated_at TIMESTAMP, processed_at TIMESTAMP
) USING DELTA""")
valid.createOrReplaceTempView("_valid_transactions")
spark.sql(f"""MERGE INTO {transactions} t USING _valid_transactions s ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""")

# COMMAND ----------

rejected = latest.where(F.col("quality_reason").isNotNull()).select(
    *raw_columns, "quality_reason", F.current_timestamp().alias("rejected_at"))
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine} (
 transaction_id STRING, customer_id STRING, product_id STRING, event_ts STRING, amount STRING,
 payment_channel STRING, device_id STRING, is_fraud STRING, source_batch_id STRING, updated_at STRING,
 quality_reason STRING, rejected_at TIMESTAMP
) USING DELTA""")
rejected.createOrReplaceTempView("_rejected_transactions")
spark.sql(f"""MERGE INTO {quarantine} t USING _rejected_transactions s
ON t.transaction_id <=> s.transaction_id AND t.source_batch_id <=> s.source_batch_id AND t.quality_reason = s.quality_reason
WHEN NOT MATCHED THEN INSERT *""")
display(spark.table(quarantine).groupBy("source_batch_id", "quality_reason").count().orderBy("source_batch_id", "quality_reason"))
display(spark.table(transactions).groupBy("source_batch_id").count().orderBy("source_batch_id"))
display(spark.sql(f"DESCRIBE HISTORY {transactions}"))