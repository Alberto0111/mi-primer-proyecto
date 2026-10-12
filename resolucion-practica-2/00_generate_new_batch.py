# Databricks notebook source
# MAGIC %md
# MAGIC # Generar un nuevo lote de entrada
# MAGIC
# MAGIC Se ejecuta **fuera del Job**. Crea un único CSV estable para demostrar procesamiento incremental, una corrección, un duplicado exacto y registros inválidos. Cambiá `batch_id` para una prueba posterior.

# COMMAND ----------

dbutils.widgets.text("student_id", "alberto", "01 - Identificador")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"], "02 - Escala")
dbutils.widgets.text("batch_id", "batch_003", "03 - Lote")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/generate_incremental_data

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
create_course_namespace(spark, config)
batch_id = dbutils.widgets.get("batch_id")
result = write_incremental_batch(dbutils, config, batch_id)
display(spark.createDataFrame([result]))
display(spark.createDataFrame(build_incremental_rows(config, batch_id)))
print("Archivo listo. Ahora ejecutá el Job con expected_batch_id igual a", batch_id)

# COMMAND ----------

