Nombre: Alberto González Rodríguez<br>
student_id: alberto<br>

# Respuestas a la consigna de la actividad

## Observaciones sobre los formatos

1) Inferencia vs. Tipado Físico (amount como texto): Al leer el CSV de transacciones de forma nativa, Spark infiere la columna amount como string. Esto ocurre porque el archivo contiene ruido (valores "N/A"). Forzar inferSchema=true soluciona tipos limpios como enteros o fechas, pero mantiene amount como texto debido a la presencia de estas cadenas. Además, la inferencia requiere una lectura adicional de los datos, lo que penaliza el rendimiento y genera esquemas inestables si los datos futuros cambian. En contraste, Parquet (products) mantiene un esquema físico nativo estricto (ej. decimal(12,2)) que no requiere inferencia.
  
2) Metadatos de Ingesta: La capa Bronze enriquece los datos añadiendo columnas de trazabilidad (_source, _source_file y _ingested_at) sin alterar la semántica original del negocio, garantizando el linaje del dato.

3) El valor agregado de Delta vs. Parquet: Mediante los comandos DESCRIBE DETAIL e HISTORY, se comprueba que Delta añade sobre Parquet un registro de transacciones (ACID Compliance), capacidades de Time Travel (historias de versiones), vector de eliminación (deletion vectors) y estadísticas avanzadas de archivos directamente accesibles sin necesidad de escanear todo el almacenamiento.

## Análisis de las 5 Vs en este caso ...

Volumen: Se procesan estructuras de tamaño considerable en pocos segundos (ej. 200,000 eventos y más de 50,000 transacciones en escala pequeña).

Velocidad: Uso del motor optimizado Photon en Databricks para procesar, agrupar y guardar planes de ejecución físicos de manera eficiente.

Variedad: Ingesta simultánea y unificada de múltiples formatos: relacionales/estructurados (Parquet), semiestructurados (JSON con estructuras anidadas struct) y planos con texto plano (CSV).

Veracidad: Identificación explícita de problemas de calidad en Bronze (52 importes inválidos y 11 IDs duplicados) sin eliminarlos, reflejando fielmente la incertidumbre del origen.

Valor: La estructuración organizada en tablas Delta y la trazabilidad inyectada preparan el terreno para que los datos ruidosos se transformen en información confiable para el negocio en las capas siguientes.
