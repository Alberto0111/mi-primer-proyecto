# Databricks notebook source
# MAGIC %md
# MAGIC # 3 — Construcción de Gold
# MAGIC
# MAGIC Gold publica tres productos con granos explícitos: ventas diarias, riesgo por cliente y resumen operativo por lote. Se reconstruyen desde Silver para que una reejecución sea determinista.

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
tx = qualified_table(config, "silver_transactions")
customers = qualified_table(config, "silver_customers")
products = qualified_table(config, "silver_products")
quarantine = qualified_table(config, "silver_transactions_quarantine")
for name in [tx, customers, products, quarantine]:
    assert spark.catalog.tableExists(name), f"Falta {name}. Ejecutá la tarea 02."

# COMMAND ----------

daily = qualified_table(config, "gold_daily_sales")
spark.sql(f"""
CREATE OR REPLACE TABLE {daily} USING DELTA AS
SELECT to_date(t.event_ts) sale_date, c.country, p.category, t.payment_channel,
       count(*) transaction_count, count(DISTINCT t.customer_id) unique_customers,
       cast(sum(t.amount) AS DECIMAL(18,2)) total_amount,
       cast(avg(t.amount) AS DECIMAL(18,2)) average_amount,
       sum(t.is_fraud) fraud_transactions,
       cast(sum(t.is_fraud) / count(*) AS DECIMAL(8,4)) fraud_rate
FROM {tx} t
JOIN {customers} c ON t.customer_id = c.customer_id
JOIN {products} p ON t.product_id = p.product_id
GROUP BY to_date(t.event_ts), c.country, p.category, t.payment_channel
""")

# COMMAND ----------

risk = qualified_table(config, "gold_customer_risk")
spark.sql(f"""
CREATE OR REPLACE TABLE {risk} USING DELTA AS
WITH metrics AS (
  SELECT t.customer_id, c.country, c.segment, count(*) transaction_count,
         cast(sum(t.amount) AS DECIMAL(18,2)) total_amount,
         cast(avg(t.amount) AS DECIMAL(18,2)) average_amount,
         count(DISTINCT t.device_id) distinct_devices, sum(t.is_fraud) fraud_transactions,
         max(t.event_ts) last_transaction_at
  FROM {tx} t JOIN {customers} c ON t.customer_id = c.customer_id
  GROUP BY t.customer_id, c.country, c.segment
)
SELECT *, CASE WHEN fraud_transactions >= 2 THEN 'high'
               WHEN fraud_transactions = 1 OR average_amount >= 1500 THEN 'medium'
               ELSE 'low' END risk_level
FROM metrics
""")

# COMMAND ----------

batch = qualified_table(config, "gold_batch_summary")
spark.sql(f"""
CREATE OR REPLACE TABLE {batch} USING DELTA AS
WITH accepted AS (
  SELECT source_batch_id, count(*) accepted_transactions,
         cast(sum(amount) AS DECIMAL(18,2)) total_amount, sum(is_fraud) fraud_transactions
  FROM {tx} GROUP BY source_batch_id
), rejected AS (
  SELECT source_batch_id, count(*) rejected_transactions FROM {quarantine} GROUP BY source_batch_id
)
SELECT coalesce(a.source_batch_id, r.source_batch_id) source_batch_id,
       coalesce(a.accepted_transactions, 0) accepted_transactions,
       coalesce(r.rejected_transactions, 0) rejected_transactions,
       coalesce(a.total_amount, cast(0 AS DECIMAL(18,2))) total_amount,
       coalesce(a.fraud_transactions, 0) fraud_transactions, current_timestamp() refreshed_at
FROM accepted a FULL OUTER JOIN rejected r ON a.source_batch_id = r.source_batch_id
""")
display(spark.table(batch).orderBy("source_batch_id"))
display(spark.table(daily).orderBy("sale_date", "country").limit(20))