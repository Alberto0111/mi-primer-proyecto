# Databricks notebook source
# MAGIC %md
# MAGIC # Práctica 2 — Verificación de Bronze
# MAGIC
# MAGIC Este notebook confirma que la salida de la práctica 1 existe y corresponde a la misma escala. No modifica las tablas Bronze.

# COMMAND ----------

dbutils.widgets.text("student_id", "alberto", "01 - Identificador")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"], "02 - Escala")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
create_course_namespace(spark, config)
required = ["bronze_customers", "bronze_products", "bronze_transactions", "bronze_events"]
missing = [name for name in required if not spark.catalog.tableExists(qualified_table(config, name))]
assert not missing, f"Faltan tablas de la práctica 1: {missing}. Ejecutá primero clase-01."
counts = {name: spark.table(qualified_table(config, name)).count() for name in required}
assert counts["bronze_customers"] == config.rows["customers"], "student_id o scale no coincide con la práctica 1"
assert counts["bronze_products"] == config.rows["products"], "student_id o scale no coincide con la práctica 1"
display(spark.createDataFrame([(name, rows) for name, rows in counts.items()], ["tabla", "filas"]))
print(f"Listo: {config.namespace}; entrada incremental: {config.volume_path}/incoming/transactions/")

# COMMAND ----------

