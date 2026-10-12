* **Nombre:** Alberto González
* **Student ID:** alberto
* **Escala:** small

<img width="947" height="130" alt="Job_clase2" src="https://github.com/user-attachments/assets/c1531a48-b430-4f2e-bf2c-991d541e74f7" />

**Nombre exacto del Job:** bigdata_alberto_silver_gold

###  Métricas de la Primera y Segunda Ejecución
* **Resultados de la Primera Ejecución (`batch_002`):** El pipeline procesó el nuevo lote incremental, integrando las filas válidas en Silver, derivando las inconsistencias a Cuarentena y actualizando los reportes Gold. La tabla `pipeline_run_audit` registró la primera fila operativa de control.
* **Resultados de la Segunda Ejecución (Prueba de Idempotencia):** Al ejecutar el Job consecutivamente sin nuevos archivos, `COPY INTO` omitió la carga física y `MERGE` no duplicó claves lógicas. Las métricas de registros en Silver y Gold permanecieron completamente estables y la auditoría marcó `idempotence_compared = true`.

### Balance Operativo del Lote `batch_002`
* **Transacciones Aceptadas (Silver):** 200 filas
* **Transacciones Rechazadas (Cuarentena):** 2 filas
* **Total de registros físicos recibidos:** 202 filas

### Diferencia Funcional: `COPY INTO` vs. `MERGE`
* **`COPY INTO` (Nivel de Archivo / Capa Bronze):** Es un comando de ingesta incremental enfocado en la infraestructura de archivos. Registra el historial y linaje de los archivos físicos cargados para evitar procesar dos veces el mismo documento CSV, garantizando la eficiencia en la entrada cruda.
* **`MERGE` (Nivel de Registro / Capa Silver):** Es una operación lógica relacional enfocada en la entidad de negocio. Evalúa fila por fila usando la clave primaria (`transaction_id`) para decidir si inserta un nuevo registro o actualiza uno existente, impidiendo la duplicidad de transacciones.

### Preguntas Notebook 05
<img width="1089" height="490" alt="image" src="https://github.com/user-attachments/assets/e71908bd-0c6f-43d6-898f-aa3a2ccce0f2" />

**Pregunta:** ¿Qué día tuvo el mayor monto vendido y ese día también fue el de mayor cantidad de transacciones?
* **Día con mayor monto vendido:** 2026-03-01 ($7,310,840.77)
* **Día con mayor cantidad de transacciones:** 2026-03-01 (7,236 txs)
* **Respuesta:** SÍ coinciden. El día 2026-03-01 fue el de mayor recaudación y mayor volumen de transacciones.
* **CONCLUSIÓN:** El ticket promedio del cliente se mantuvo estable o constante, y la empresa recaudó más simplemente porque procesó más transacciones ese día.

<img width="989" height="490" alt="image" src="https://github.com/user-attachments/assets/eab16c8a-6b3d-4220-985d-835ae2dad92d" />

**Pregunta:** ¿Qué canal de pago presenta la mayor tasa de fraude? ¿La conclusión se sostiene al considerar el número de transacciones de cada canal?

**--- Métricas Consolidadas por Canal de Pago ---**  
* Canal: transfer | Tasa Fraude: 13.9150% | Volumen: 16,723 txs (Fraudes: 2,327)
* Canal: wallet | Tasa Fraude: 12.9694% | Volumen: 16,724 txs (Fraudes: 2,169)
* Canal: card | Tasa Fraude: 12.5370% | Volumen: 16,902 txs (Fraudes: 2,119)
* **Respuesta corta:** El canal con mayor tasa de fraude es transfer (13.9150%).
* **CONCLUSIÓN:** Sí, la conclusión se sostiene. Al analizar el denominador (el volumen de actividad), observamos que la distribución de transacciones está perfectamente balanceada entre los tres canales; la tasa de fraude no está distorsionada por un bajo número de muestras (ruido estadístico).

<img width="927" height="590" alt="image" src="https://github.com/user-attachments/assets/76f4c296-70f3-42df-865e-87a5a2e81947" />

**Pregunta:** ¿Qué combinación de país y categoría genera el mayor monto? ¿Existe una categoría dominante en todos los países o cambia según el mercado?

**--- Máximo Global ---**
* La combinación que genera el mayor monto es: País: BR | Categoría: home ($2,361,959.97)
* **CONCLUSIÓN:** A pesar de las variaciones locales, el mapa de calor revela un patrón claro en la demanda de la región: las categorías home y books actúan como los dos grandes motores comerciales en todos los países

<img width="790" height="490" alt="image" src="https://github.com/user-attachments/assets/90ccd6f8-c149-4e3e-a5df-871693a7540c" />

**Pregunta:** ¿Qué proporción de cada lote fue aceptada y rechazada? ¿El lote nuevo presenta una calidad diferente del lote inicial?

**--- Resumen de Métricas por Lote ---**

Lote: batch_002 | Total: 202 txs | Aceptadas: 99.01% | Rechazadas: 0.99%\
Lote: batch_003 | Total: 203 txs | Aceptadas: 99.01% | Rechazadas: 0.99%\
Lote: initial | Total: 49,999 txs | Aceptadas: 99.90% | Rechazadas: 0.10%
* **CONCLUSIÓN:** La calidad de los lotes nuevos empeoró drásticamente en comparación con el lote inicial. Los lotes batch_002 y batch_003 presentan una tasa de rechazo del 0.99%, una cifra diez veces mayor que el 0.10% del lote initial.

##  Respuestas al Cuestionario de Análisis y Comprensión

#### 1. ¿Cuántas filas físicas recibió cada lote en `bronze_transactions_incremental`?
**Consulta:**
```sql
SELECT source_batch_id, count(*) AS filas_fisicas 
FROM bronze_transactions_incremental 
GROUP BY source_batch_id ORDER BY source_batch_id;
```
**Respuesta:** El lote batch_002 recibió un total de 204 filas físicas, al igual que el lote batch_003 que registró exactamente 204 filas. Esto demuestra un ingreso volumétrico simétrico y estabilizado en la ingesta incremental cruda de Bronze antes de las depuraciones.

#### 2. Para cada lote, ¿cuántas transacciones fueron aceptadas y cuántas quedaron en `silver_transactions_quarantine`? Reconciliá tus resultados con `gold_batch_summary`.
**Consulta:**
```sql
SELECT source_batch_id, accepted_transactions, rejected_transactions 
FROM gold_batch_summary ORDER BY source_batch_id;
```
**Respuesta:** Para el lote batch_002 se registraron 200 aceptadas y 2 rechazadas; para el lote batch_003 figuraron 201 aceptadas y 2 rechazadas, mientras que el lote initial cerró con 49,948 aceptadas y 51 rechazadas. Estos datos consolidados garantizan la perfecta trazabilidad y reconciliación analítica del pipeline.

#### 3. ¿Qué motivos de rechazo aparecen en la cuarentena y cuántos registros tiene cada uno por lote? ¿Los rechazos observados coinciden con los casos introducidos por el generador?
**Consulta:**
```sql
SELECT source_batch_id, quality_reason, count(*) 
FROM silver_transactions_quarantine 
GROUP BY ALL ORDER BY source_batch_id;
```
**Respuesta:** En batch_002 aparece 1 registro por UNKNOWN_CUSTOMER y 1 por INVALID_AMOUNT; en batch_003 se repite el mismo patrón con 1 registro para cada motivo, mientras que el lote initial concentra 51 registros rechazados exclusivamente por INVALID_AMOUNT. Estos resultados validan la efectividad de los filtros ante anomalías tipográficas y de integridad referencial.

#### 4. Seguí la transacción `42` desde `bronze_transactions_all` hasta `silver_transactions`. ¿Cuántas versiones existen en Bronze y cuál quedó vigente en Silver? Mostrá las columnas que justifican la elección.
**Consulta:**
```sql
SELECT source_batch_id, amount, updated_at FROM bronze_transactions_all WHERE transaction_id = '42';
SELECT amount, source_batch_id, updated_at FROM silver_transactions WHERE transaction_id = 42;
```
**Respuesta:** En la vista de Bronze existen tres versiones de la transacción 42: la de initial ($600.11), la de batch_002 ($1999.99) y la de batch_003 ($1999.99). En la capa Silver quedó vigente la versión de batch_003 con un importe de 1999.99 debido a que poseía la marca temporal updated_at más reciente (2026-03-11) de todo el historial.

#### 5. Comprobá mediante una consulta que `silver_transactions` tiene una sola fila por `transaction_id`. ¿Qué resultado indicaría que la deduplicación falló?
**Consulta:**
```sql
SELECT transaction_id, count(*) FROM silver_transactions 
GROUP BY transaction_id HAVING count(*) > 1;
```
**Respuesta:** La consulta devuelve cero filas, lo que comprueba la unicidad e integridad de la clave primaria de negocio. Si la deduplicación hubiese fallado, la salida expondría identificadores con conteos mayores o iguales a dos, indicando duplicados en producción.

#### 6. Calculá la tasa de rechazo de cada lote como `rechazadas / (aceptadas + rechazadas)` en `gold_batch_summary`. ¿Es correcto comparar solamente las cantidades absolutas si los lotes tienen tamaños diferentes?
**Consulta:**
```sql
SELECT source_batch_id, (rejected_transactions / (accepted_transactions + rejected_transactions)) * 100 AS tasa_rechazo 
FROM gold_batch_summary;
```
**Respuesta:** El lote histórico arroja una tasa de rechazo de 0.10% frente a un 0.99% de los lotes nuevos. No es correcto evaluar solo números absolutos debido al sesgo de escala: el lote inicial acumula más errores brutos por su tamaño, pero operativamente es diez veces más limpio que las cargas nuevas.

#### 7. ¿Qué día presenta el mayor monto total y cuál presenta la mayor cantidad de transacciones? Consultá `gold_daily_sales` y explicá si ambos máximos coinciden.
**Consulta:**
```sql
SELECT sale_date, sum(total_amount), sum(transaction_count) 
FROM gold_daily_sales GROUP BY sale_date ORDER BY 2 DESC;
```
**Respuesta:** El día de máxima recaudación financiera coincide exactamente con la fecha de mayor volumen operativo de órdenes de la escala (2026-03-01). Esto demuestra un escenario homogéneo donde el ticket promedio por cliente se mantuvo estable y el pico de ingresos se explicó por la cantidad neta de operaciones comerciales.

#### 8. ¿Qué canal de pago tiene la mayor tasa global de fraude? Calculala como SUM(fraud_transactions) / SUM(transaction_count) y explicá por qué no corresponde promediar directamente fraud_rate.
**Consulta:**
```sql
SELECT payment_channel, sum(fraud_transactions) / sum(transaction_count) * 100 AS tasa_fraude_real
FROM gold_daily_sales GROUP BY payment_channel ORDER BY 2 DESC;
``` 
**Respuesta:** El canal con mayor riesgo es transfer con una tasa de fraude consolidada del 13.9150%. No corresponde promediar directamente las tasas parciales diarias de fraud_rate porque cada grupo responde a un volumen de transacciones diferente, lo que generaría un error matemático al no ponderar proporcionalmente las bases.

#### 9. ¿Qué combinación de país y categoría concentra el mayor monto vendido? Mostrá también la combinación líder dentro de cada país.
**Consulta:**
```sql
SELECT country, category, sum(total_amount)
FROM gold_daily_sales GROUP BY ALL ORDER BY 3 DESC; 
```
**Respuesta:** El máximo regional absoluto se concentra en Brasil (BR) bajo el rubro de productos home con $2,361,960. El liderazgo interno varía geográficamente: en Argentina y México la delantera la toma books, mientras que en Brasil, Chile y Uruguay domina la categoría home.

#### 10. Compará las dos primeras filas de pipeline_run_audit correspondientes a la reejecución de batch_002. ¿Qué métricas permanecen iguales y qué columna demuestra que se realizó la comparación de idempotencia?
**Consulta:**
```sql
SELECT expected_batch_id, silver_rows, quarantine_rows, gold_rows, idempotence_compared
FROM pipeline_run_audit ORDER BY recorded_at DESC LIMIT 2; 
```
**Respuesta:** Al realizar la reejecución del lote batch_002, las métricas permanecieron completamente estables e iguales con 50,149 filas en Silver, 53 en Cuarentena y 598 en Gold. La columna que demuestra que el pipeline realizó con éxito el control y validación de duplicados es idempotence_compared = true; mientras que al procesar un lote nuevo con más datos (batch_003), dicha columna cambia correctamente a false al detectar el incremento de volumen (50,349 filas en Silver).

## Interpretación del código y del pipeline

#### 11. En 01_ingest_bronze_incremental.ipynb, ¿qué problema resuelve COPY INTO y qué información utiliza para evitar cargar dos veces el mismo archivo físico?

**Respuesta:** El comando COPY INTO resuelve el problema de la sobrecarga y duplicidad de datos en la capa Bronze ante ejecuciones repetidas del pipeline sobre un mismo lote. Utiliza un registro interno de metadatos de Databricks para indexar las firmas y fechas de modificación de los archivos del volumen, omitiendo de forma inteligente aquellos ya procesados.

#### 12. ¿Por qué la vista bronze_transactions_all usa UNION ALL en lugar de eliminar duplicados? ¿En qué capa se resuelven los duplicados de negocio y por qué?

**Respuesta:** Se utiliza UNION ALL porque la capa Bronze tiene la política de resguardar el dato histórico en bruto exactamente como se recibió del origen, sin aplicar alteraciones. Los duplicados lógicos se resuelven centralizadamente en la capa Silver, garantizando un único punto de verdad auditado antes de generar los reportes de negocio.

#### 13. ¿Por qué las transacciones iniciales reciben source_batch_id='initial' y usan event_ts como updated_at? ¿Cómo afecta eso a la corrección de la transacción 42?

**Respuesta:** Se configuran así para establecer una línea de tiempo base histórica clara frente a las cargas incrementales futuras. Esto permite que el MERGE identifique que la nueva versión de la transacción 42 (proveniente de un lote nuevo) posee un updated_at superior al histórico, provocando la actualización del importe en lugar de su descarte.

#### 14. En quality_rules.py, ¿qué ventaja ofrece try_cast frente a un cast convencional cuando llega un importe como N/A?

**Respuesta:** A diferencia de un cast clásico que arroja una excepción fatal interrumpiendo el procesamiento de todo el clúster de Spark al encontrar un texto alfanumérico, el método try_cast gestiona el error de forma segura. Convierte el valor ruidoso (como "N/A") en un estado NULL, permitiendo que el registro sea identificado y derivado a la cuarentena.

#### 15. Las reglas de calidad asignan una única quality_reason. ¿Qué sucede si un registro viola más de una regla y por qué importa el orden de las condiciones?

**Respuesta:** Si un registro quiebra múltiples reglas de negocio, el pipeline solo documentará la primera infracción evaluada debido al comportamiento de cortocircuito lógico del código. Por este motivo, el orden de las condiciones es fundamental: se deben anteponer siempre las validaciones estructurales críticas (como fallos de casteo) antes que los cruces relacionales secundarios.

#### 16. Explicá cómo se construye _record_key y cómo se usa junto con row_number. ¿Qué caso cubre el hash cuando transaction_id no puede convertirse a un número?

**Respuesta:** Se construye aplicando la función sha2 criptográfica sobre la combinación de todas las columnas crudas de la fila, asignando una huella digital única a cada registro. Se utiliza junto con la ventana analítica row_number para priorizar y aislar la versión más reciente del dato. El hash cubre de forma segura el escenario donde el transaction_id viene corrupto o nulo, sirviendo como un ancla técnica para poder procesar la fila.

#### 17. Interpretá las dos cláusulas principales del MERGE de silver_transactions. ¿Cuándo se actualiza una fila existente y cuándo se inserta una nueva?

**Respuesta:** La cláusula WHEN MATCHED determina que una transacción existente en Silver se actualizará únicamente si el registro entrante posee una marca temporal updated_at estrictamente más reciente que la guardada. La cláusula WHEN NOT MATCHED se activa de forma directa cuando el identificador es completamente inédito en el sistema, insertándolo como una nueva fila en producción.

#### 18. ¿Por qué las tablas Gold se reconstruyen completamente en esta práctica mientras Silver se actualiza con MERGE? Mencioná una ventaja y una limitación de cada estrategia.

**Respuesta:** Las tablas Gold se reconstruyen por completo con CREATE OR REPLACE para asegurar el determinismo matemático absoluto en el cálculo de métricas agregadas complejas de series temporales. La ventaja de reconstruir Gold es su consistencia analítica, pero su limitación es el alto uso de recursos en grandes volúmenes; por el contrario, la ventaja del MERGE en Silver es su eficiencia en la escritura incremental, teniendo como límite una lógica de código más compleja de mantener.

#### 19. ¿Por qué expected_batch_id no participa en la detección del archivo nuevo? Indicá qué parte del pipeline descubre batch_003 y qué parte utiliza el parámetro.

**Respuesta:** El pipeline de datos es completamente autónomo y es el comando COPY INTO el encargado de explorar el almacenamiento de origen para descubrir e ingestar nuevos archivos de manera nativa. El parámetro expected_batch_id no interviene en el procesamiento y es utilizado únicamente al final por el módulo 04_validate_pipeline.ipynb para validar si los resultados finales de la corrida coinciden con las expectativas de prueba del usuario.

#### 20. Si la tarea build_silver falla, ¿qué ocurre con build_gold y validate en el Job? Explicá cómo las dependencias del DAG evitan publicar o validar resultados incompletos.

**Respuesta:** Si la tarea build_silver falla, las tareas posteriores de build_gold y validate son omitidas automáticamente por el orquestador. Las dependencias secuenciales establecidas en el grafo acíclico dirigido (DAG) actúan como una barrera de seguridad, impidiendo que el negocio consuma reportes incompletos o desactualizados y dejando el Job general en estado de alerta (color rojo).
