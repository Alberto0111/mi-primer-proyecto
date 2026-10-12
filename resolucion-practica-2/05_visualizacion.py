# Databricks notebook source
# MAGIC %md
# MAGIC # 5 — Visualización orientada a preguntas
# MAGIC
# MAGIC Un gráfico no es el objetivo: es evidencia para responder una pregunta. En cada ejercicio primero escribí qué querés averiguar, luego construí la agregación y finalmente redactá una conclusión de dos o tres oraciones.
# MAGIC
# MAGIC Este notebook se ejecuta manualmente **después** de que el Job termina correctamente; no forma parte del pipeline.

# COMMAND ----------

dbutils.widgets.text("student_id", "alberto", "01 - Identificador")
dbutils.widgets.dropdown("scale", "small", ["test", "small", "demo"], "02 - Escala")

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

catalog = spark.sql("SELECT current_catalog()").first()[0]
config = CourseConfig(catalog, dbutils.widgets.get("student_id"), dbutils.widgets.get("scale"))
daily = qualified_table(config, "gold_daily_sales")
risk = qualified_table(config, "gold_customer_risk")
batch = qualified_table(config, "gold_batch_summary")
for table in [daily, risk, batch]:
    assert spark.catalog.tableExists(table), f"Falta {table}. Ejecutá primero el Job completo."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejemplo resuelto
# MAGIC
# MAGIC **Pregunta:** ¿Qué categoría concentra el mayor monto vendido?
# MAGIC
# MAGIC La agregación se hace con Spark. Sólo el resultado pequeño se convierte a Pandas para dibujarlo. Un gráfico de barras ordenado permite comparar categorías; un gráfico de torta haría más difícil comparar valores cercanos.

# COMMAND ----------

category_sales = (spark.table(daily)
    .groupBy("category")
    .agg(F.sum("total_amount").alias("total_amount"))
    .orderBy(F.desc("total_amount")))

plot_data = category_sales.toPandas().sort_values("total_amount")
fig, ax = plt.subplots(figsize=(9, 4.5))
ax.barh(plot_data["category"], plot_data["total_amount"], color="#2563eb")
ax.set_title("Monto vendido por categoría")
ax.set_xlabel("Monto total")
ax.set_ylabel("Categoría")
ax.grid(axis="x", alpha=0.25)
fig.tight_layout()
plt.show()

leader = category_sales.first()
print(f"Respuesta: {leader.category} es la categoría con mayor monto vendido ({leader.total_amount:,.2f}).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consigna 1 — Evolución temporal
# MAGIC
# MAGIC **Pregunta:** ¿Qué día tuvo el mayor monto vendido y ese día también fue el de mayor cantidad de transacciones?
# MAGIC
# MAGIC Construí una visualización temporal que permita comparar ambas métricas sin ocultar sus unidades. Explicá si los dos máximos coinciden y qué implica que coincidan —o no— sobre el ticket promedio. Fuente sugerida: `gold_daily_sales`.

# COMMAND ----------

# Tu solución. Agregá por sale_date antes de convertir el resultado a Pandas.
# Incluí título, ejes, unidades y una respuesta escrita.
daily_metrics = (spark.table(daily)
    .groupBy("sale_date")
    .agg(
        F.sum("total_amount").alias("monto_total"),
        F.sum("transaction_count").alias("transacciones_totales")
    )
    .orderBy("sale_date"))
df_plot = daily_metrics.toPandas().sort_values("sale_date")
fig, ax1 = plt.subplots(figsize=(11, 5))
color_monto = "#2563eb"  # Azul
ax1.set_xlabel("Fecha de Venta (sale_date)")
ax1.set_ylabel("Monto Total Vendido ($)", color=color_monto)
line1 = ax1.plot(df_plot["sale_date"], df_plot["monto_total"], color=color_monto, marker='o', linewidth=2, label="Monto Total")
ax1.tick_params(axis='y', labelcolor=color_monto)
ax1.grid(True, alpha=0.25)
ax2 = ax1.twinx()  
color_tx = "#10b981"  # Verde
ax2.set_ylabel("Cantidad de Transacciones", color=color_tx)
line2 = ax2.plot(df_plot["sale_date"], df_plot["transacciones_totales"], color=color_tx, marker='s', linestyle='--', linewidth=2, label="Transacciones")
ax2.tick_params(axis='y', labelcolor=color_tx)
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="upper left")
plt.title("Evolución Temporal: Monto Vendido vs Cantidad de Transacciones")
fig.tight_layout()
plt.show()
max_monto_row = daily_metrics.orderBy(F.desc("monto_total")).first()
max_tx_row = daily_metrics.orderBy(F.desc("transacciones_totales")).first()

print(f"Día con mayor monto vendido: {max_monto_row.sale_date} (${max_monto_row.monto_total:,.2f})")
print(f"Día con mayor cantidad de transacciones: {max_tx_row.sale_date} ({max_tx_row.transacciones_totales:,} txs)")

if max_monto_row.sale_date == max_tx_row.sale_date:
    print(f"\nRespuesta: SÍ coinciden. El día {max_monto_row.sale_date} fue el de mayor recaudación y mayor volumen de transacciones.")
else:
    print(f"\nRespuesta: NO coinciden. El pico de recaudación fue el {max_monto_row.sale_date}, mientras que el pico de volumen de transacciones fue el {max_tx_row.sale_date}.")

# COMMAND ----------

# MAGIC %md
# MAGIC CONCLUSIÓN: El ticket promedio del cliente se mantuvo estable o constante, y la empresa recaudó más simplemente porque procesó más transacciones ese día.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consigna 2 — Canal y fraude
# MAGIC
# MAGIC **Pregunta:** ¿Qué canal de pago presenta la mayor tasa de fraude? ¿La conclusión se sostiene al considerar el número de transacciones de cada canal?
# MAGIC
# MAGIC Mostrá tasa y volumen de manera legible. No compares sólo cantidades de fraude: calculá el denominador correcto. Fuente sugerida: `gold_daily_sales`.

# COMMAND ----------

# Tu solución. Recalculá la tasa global como suma(fraud_transactions) / suma(transaction_count).
# Evitá promediar directamente fraud_rate entre grupos.
channel_fraud = (spark.table(daily)
    .groupBy("payment_channel")
    .agg(
        F.sum("transaction_count").alias("total_transactions"),
        F.sum("fraud_transactions").alias("total_fraud_transactions")
    )
    # Calculamos la tasa de fraude consolidada (fraudes / transacciones totales)
    .withColumn("fraud_rate_consolidated", F.col("total_fraud_transactions") / F.col("total_transactions"))
    .orderBy(F.desc("fraud_rate_consolidated")))

df_fraud = channel_fraud.toPandas()
fig, ax1 = plt.subplots(figsize=(10, 5))
color_vol = "#94a3b8"  # Gris azulado para el volumen de fondo
ax1.set_xlabel("Canal de Pago (payment_channel)")
ax1.set_ylabel("Volumen Total de Transacciones", color="#475569")
bars = ax1.bar(df_fraud["payment_channel"], df_fraud["total_transactions"], color=color_vol, width=0.5, label="Volumen Transacciones")
ax1.tick_params(axis='y', labelcolor="#475569")
ax1.grid(axis="y", alpha=0.2)
ax2 = ax1.twinx()
color_tasa = "#dc2626"  # Rojo para resaltar el riesgo de fraude
ax2.set_ylabel("Tasa de Fraude Consolidada (Fraudes / Total)", color=color_tasa)
line = ax2.plot(df_fraud["payment_channel"], df_fraud["fraud_rate_consolidated"], color=color_tasa, marker='o', linewidth=2.5, label="Tasa de Fraude")
ax2.tick_params(axis='y', labelcolor=color_tasa)
import matplotlib.ticker as mtick
ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

plt.title("Análisis de Riesgo: Tasa de Fraude vs. Volumen por Canal de Pago")
fig.tight_layout()
plt.show()
highest_rate_row = channel_fraud.first()
print("--- Métricas Consolidadas por Canal de Pago ---")
for index, row in df_fraud.iterrows():
    print(f"Canal: {row['payment_channel']} | Tasa Fraude: {row['fraud_rate_consolidated']*100:.4f}% | Volumen: {row['total_transactions']:,} txs (Fraudes: {row['total_fraud_transactions']:,})")

print(f"\nRespuesta corta: El canal con mayor tasa de fraude es {highest_rate_row.payment_channel} ({highest_rate_row.fraud_rate_consolidated*100:.4f}%).")

# COMMAND ----------

# MAGIC %md
# MAGIC CONCLUSIÓN: Sí, la conclusión se sostiene. Al analizar el denominador (el volumen de actividad), observamos que la distribución de transacciones está perfectamente balanceada entre los tres canales; la tasa de fraude no está distorsionada por un bajo número de muestras (ruido estadístico).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consigna 3 — Concentración geográfica y de producto
# MAGIC
# MAGIC **Pregunta:** ¿Qué combinación de país y categoría genera el mayor monto? ¿Existe una categoría dominante en todos los países o cambia según el mercado?
# MAGIC
# MAGIC Elegí una visualización que permita comparar simultáneamente países y categorías. Justificá brevemente por qué ese tipo de gráfico es adecuado. Fuente sugerida: `gold_daily_sales`.

# COMMAND ----------

# MAGIC %pip install seaborn

# COMMAND ----------

# Tu solución. Algunas opciones: mapa de calor o barras agrupadas.
# La conclusión debe mencionar tanto el máximo global como las diferencias entre países.

import seaborn as sns
geo_category_sales = (spark.table(daily)
    .groupBy("country", "category")
    .agg(F.sum("total_amount").alias("monto_total"))
    .orderBy(F.desc("monto_total")))
df_geo = geo_category_sales.toPandas()
df_pivot = df_geo.pivot(index="country", columns="category", values="monto_total")
df_pivot = df_pivot.fillna(0).astype(float)
plt.figure(figsize=(10, 6))
sns.heatmap(
    df_pivot, 
    annot=True,          # Muestra los valores numéricos dentro de cada celda
    fmt=",.0f",          # Formato de número con separador de miles y sin decimales
    cmap="YlGnBu",       # Paleta de colores atractiva (Amarillo -> Verde -> Azul oscuro para el máximo)
    linewidths=0.5,      # Línea sutil de separación entre celdas
    cbar_kws={'label': 'Monto Total Vendido ($)'}
)

plt.title("Concentración Geográfica y de Producto: Monto Vendido por País y Categoría")
plt.xlabel("Categoría de Producto (category)")
plt.ylabel("País (country)")
plt.tight_layout()
plt.show()
max_global = geo_category_sales.first()
print(f"--- Máximo Global ---")
print(f"La combinación que genera el mayor monto es: País: {max_global.country} | Categoría: {max_global.category} (${max_global.monto_total:,.2f})")

# COMMAND ----------

# MAGIC %md
# MAGIC CONCLUSIÓN: A pesar de las variaciones locales, el mapa de calor revela un patrón claro en la demanda de la región: las categorías home y books actúan como los dos grandes motores comerciales en todos los países

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consigna 4 — Calidad del pipeline
# MAGIC
# MAGIC **Pregunta:** ¿Qué proporción de cada lote fue aceptada y rechazada? ¿El lote nuevo presenta una calidad diferente del lote inicial?
# MAGIC
# MAGIC Compará lotes usando proporciones además de cantidades absolutas. Señalá cualquier limitación de comparar lotes de tamaños distintos. Fuente sugerida: `gold_batch_summary`.

# COMMAND ----------

# Tu solución. Calculá total = accepted_transactions + rejected_transactions.
# Un gráfico de barras apiladas al 100% puede facilitar la comparación de proporciones.
batch_metrics = (spark.table(batch)
    .withColumn("total_transactions", F.col("accepted_transactions") + F.col("rejected_transactions"))
    .withColumn("pct_accepted", F.col("accepted_transactions") / F.col("total_transactions") * 100)
    .withColumn("pct_rejected", F.col("rejected_transactions") / F.col("total_transactions") * 100)
    .orderBy("source_batch_id"))
df_batch = batch_metrics.toPandas()
fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(df_batch["source_batch_id"].astype(str), df_batch["pct_accepted"], 
       label="Aceptadas (%)", color="#10b981", width=0.5)

ax.bar(df_batch["source_batch_id"].astype(str), df_batch["pct_rejected"], 
       bottom=df_batch["pct_accepted"], label="Rechazadas (%)", color="#dc2626", width=0.5)


ax.set_title("Comparación de Calidad: Proporción de Transacciones por Lote (100%)")
ax.set_xlabel("Identificador del Lote (source_batch_id)")
ax.set_ylabel("Proporción (%)")
ax.set_ylim(0, 105)
for i in range(len(df_batch)):
    ax.text(i, df_batch["pct_accepted"][i] / 2, f"{df_batch['pct_accepted'][i]:.1f}%", 
            ha='center', va='center', color='white', fontweight='bold')
    ax.text(i, df_batch["pct_accepted"][i] + (df_batch["pct_rejected"][i] / 2), 
            f"{df_batch['pct_rejected'][i]:.1f}%", ha='center', va='center', color='white', fontweight='bold')

ax.legend(loc="upper right")
ax.grid(axis="y", alpha=0.2)
plt.tight_layout()
plt.show()
print("--- Resumen de Métricas por Lote ---")
for index, row in df_batch.iterrows():
    print(f"Lote: {row['source_batch_id']} | Total: {row['total_transactions']:,} txs | Aceptadas: {row['pct_accepted']:.2f}% | Rechazadas: {row['pct_rejected']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC CONCLUSIÓN: La calidad de los lotes nuevos empeoró drásticamente en comparación con el lote inicial. Los lotes batch_002 y batch_003 presentan una tasa de rechazo del 0.99%, una cifra diez veces mayor que el 0.10% del lote initial.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criterios de entrega
# MAGIC
# MAGIC Para cada consigna entregá: (1) la transformación de datos, (2) un gráfico, y (3) una conclusión de dos o tres oraciones que responda explícitamente la pregunta.
# MAGIC
# MAGIC Cada gráfico debe tener título informativo, ejes y unidades legibles, categorías ordenadas cuando corresponda y una elección de color que no sea la única forma de comunicar el significado. No conviertas una tabla Silver completa a Pandas: agregá primero con Spark y convertí sólo el resultado pequeño.