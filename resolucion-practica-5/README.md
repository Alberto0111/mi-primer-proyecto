# Entrega práctica 5 — MLOps con MLflow

- **Nombre:** Alberto Mario González Rodríguez
- **student_id:** alberto
- **Escala:** small

## Mis runs

| run | modelo | features | class_weight | umbral | hiperparametros | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| max_depth=3 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=3 | 0.602 | 0.586 | 0.809 | 0.329 | 0.808 | 0.468 |
| run4 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=sin_limite | 0.674 | 0.585 | 0.918 | 0.7 | 0.372 | 0.485 |
| max_depth=5 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=5 | 0.614 | 0.579 | 0.802 | 0.32 | 0.8 | 0.457 |
| max_depth=sin_limite | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=sin_limite | 0.62 | 0.578 | 0.811 | 0.33 | 0.799 | 0.467 |
| run3 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=sin_limite | 0.62 | 0.578 | 0.811 | 0.33 | 0.799 | 0.467 |
| max_depth=8 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=8 | 0.617 | 0.577 | 0.81 | 0.328 | 0.793 | 0.464 |
| run5 | random_forest | completas | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.914 | 0.799 | 0.226 | 0.352 |
| run2 | random_forest | completas | balanced | 0.5 | n_estimators=200, max_depth=sin_limite | 1.0 | 0.535 | 0.914 | 0.713 | 0.282 | 0.405 |
| 01_repro_b | random_forest | cliente | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.594 | 0.51 | 0.909 | 0.722 | 0.207 | 0.321 |
| 01_repro_a | random_forest | cliente | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.594 | 0.51 | 0.909 | 0.722 | 0.207 | 0.321 |
| logistic_regression-basicas-u0.5 | logistic_regression | basicas | ninguno | 0.5 | C=1.0 | 0.341 | 0.339 | 0.896 | 0.493 | 0.045 | 0.082 |
| 01_manual_logistica | logistic_regression | basicas | ninguno | 0.5 | C=1.0 | 0.341 | 0.339 | 0.896 | 0.493 | 0.045 | 0.082 |

**Run registrado:** max_depth=3 (nombre default que utilizó el run)

## Datos y tracking

**1. ¿Cuántas filas tiene cada split de ml_features y qué porcentaje de contracargos hay en cada uno? ¿Por qué se separa por fecha y no al azar?**
El split train tiene 35,654 filas (11.05% de contracargos), test tiene 7,129 filas (10.38%) y nuevas tiene 7,165 filas (11.05%). Se separa por fecha (split temporal) para simular un entorno real donde se entrena con datos del pasado y se evalúa con transacciones futuras, evitando filtraciones de datos (data leakage).

**2. ¿Qué accuracy obtiene un "modelo" que siempre dice que no hay contracargo? ¿Por qué la práctica usa el PR-AUC como métrica principal?**
Predecir siempre "no hay contracargo" obtiene un 89.6% de accuracy en test sin detectar un solo caso real. Se usa PR-AUC como métrica principal porque con clases desbalanceadas la accuracy engaña, mientras que el PR-AUC mide qué tan bien el modelo ordena el riesgo de los casos positivos sin depender de un umbral fijo.

**3. ¿Qué quedó registrado en el run 01_manual_logistica como parámetros, métricas y artefactos? ¿Qué diferencia hay entre un experimento y un run?**
En 01_manual_logistica quedaron registrados 7 parámetros (model_type, feature_set, C, class_weight, threshold, data_table, data_version), 8 métricas (test_pr_auc=0.339, test_accuracy=0.896, train_pr_auc=0.341, entre otras) y como artefactos la imagen matriz_confusion.png junto con el modelo serializado y su firma. Un experimento es la carpeta o contenedor que agrupa las pruebas de un problema, mientras que cada run es una ejecución individual de entrenamiento dentro de ese experimento.

**4. ¿Cuántos parámetros registró autolog y qué métricas? ¿Qué no registra autolog que sí registramos a mano?**
autolog registró 46 parámetros del Pipeline y 7 métricas de entrenamiento (training_accuracy_score, training_f1_score, training_log_loss, training_precision_score, training_recall_score, training_roc_auc, training_score). A diferencia del registro manual, autolog no registra las métricas evaluadas sobre nuestro conjunto de test ni métricas específicas del problema como el PR-AUC o el umbral elegido.

**5. ¿Dieron las mismas métricas 01_repro_a y 01_repro_b? Nombrá las cuatro cosas que hay que fijar para que un run sea reproducible.**
Sí, ambos runs dieron exactamente las mismas métricas (test_pr_auc = 0.5099, test_roc_auc = 0.8693, test_accuracy = 0.9094). Para que un run sea reproducible hay que fijar los datos (versión de la tabla), el código, la semilla aleatoria (random_state) y el entorno (versiones de las librerías).

**6. ¿Qué pasó al llamar al modelo sin la columna amount? ¿Para qué sirve la firma (signature) del modelo?**
Al llamar al modelo sin la columna amount, MLflow lanzó un error de validación de esquema (Failed to enforce schema...) frenando la predicción antes de ejecutarse. La firma (signature) define el contrato de columnas y tipos de datos que el modelo espera recibir y devolver, evitando errores silenciosos en producción.

## Tus experimentos

**7. ¿Cuál fue tu mejor run (modelo, variables, hiperparámetros y PR-AUC)? ¿Cuánto mejoró respecto de usar sólo rule_alert (solo_regla)?**
Mi mejor run fue max_depth=3 con gradient_boosting, features completas, class_weight=balanced, learning_rate=0.1 y umbral=0.5, alcanzando un PR-AUC de 0.586. Esto representa una mejora sustancial respecto a la línea de base y a los modelos simples (superando en +0.247 al PR-AUC de 0.339 de la regresión logística con variables básicas, y muy por encima de la tasa base de contracargos de 0.104 o la regla fija).

**8. Con el mismo modelo, ¿cuánto cambió el PR-AUC al pasar de basicas a cliente y a completas? ¿Qué variables resultaron más importantes en tu mejor run?**
En random_forest (n_estimators=100, max_depth=8, class_weight=ninguno), el PR-AUC subió de 0.510 con features cliente a 0.568 con completas (+0.058), y respecto a basicas (0.339 en logística) la mejora acumulada supera los +0.229 puntos. En mi mejor run (max_depth=3), las variables más importantes por permutación fueron amount (0.3265), payment_channel (0.1750), hour (0.1028), category (0.0740) y account_age_days (0.0592).

**9. Con completas, ¿qué modelo anduvo mejor: logística, bosque o boosting? ¿Por qué un modelo de árboles puede aprovechar combinaciones de variables (por ejemplo, de noche y por transferencia) que una logística no ve?**
Con features completas, el mejor algoritmo fue gradient_boosting (PR-AUC = 0.586), superando a random_forest (0.568) y a logistic_regression (0.339). Los modelos basados en árboles aprovechan estas combinaciones porque dividen el espacio de datos mediante reglas condicionales encadenadas (capturando interacciones no lineales), mientras que la regresión logística suma el efecto de cada variable por separado.

**10. ¿Qué cambió en precision, recall y cantidad de alertas al usar class_weight = balanced? ¿Cambió mucho el PR-AUC?**
Al activar class_weight = balanced en gradient_boosting (max_depth=sin_limite), el recall subió fuertemente de 0.372 a 0.799, la precision bajó de 0.700 a 0.330 y se dispararon muchas más alertas (de ~393 a 1,789 alertas). En cambio, el PR-AUC casi no cambió (pasó de 0.585 a 0.578), ya que el ordenamiento relativo de riesgo del modelo se mantuvo similar.

**11. ¿Qué pasó al bajar el umbral? ¿Por qué el PR-AUC no depende del umbral y el F1 sí?**
Al bajar el umbral de decisión se generan más alertas, lo que aumenta el recall pero reduce la precision. El PR-AUC no depende del umbral porque evalúa el área bajo la curva considerando todos los umbrales posibles sobre las probabilidades predichas, mientras que el F1 sí cambia porque se calcula a partir de las clasificaciones binarias concretas que resultan de cortar en un umbral específico.

**12. ¿Qué hiperparámetro barriste y con qué valor obtuviste el mejor PR-AUC de test? ¿Hubo valores con train_pr_auc mucho más alto que el de test? ¿Cómo se llama ese fenómeno?**
Barrí el hiperparámetro max_depth en gradient_boosting (balanced, completas) y obtuve el mejor PR-AUC en test con max_depth=3 (0.586). Cuando el árbol crece sin límite (max_depth=sin_limite), train_pr_auc sube a 0.620 mientras test baja a 0.578 (y en random_forest run2 llega a 1.000 en train vs. 0.535 en test); ese fenómeno se llama sobreajuste (overfitting).

## Comparar y elegir

**13. ¿Coincidieron tu mejor run por PR-AUC y tu mejor run por accuracy? Explicá por qué pueden no coincidir.**
No coincidieron: el mejor por PR-AUC fue max_depth=3 (PR-AUC = 0.586, accuracy = 0.809), mientras que el mejor por accuracy fue run4 (accuracy = 0.918, PR-AUC = 0.585). Pueden no coincidir porque con clases desbalanceadas la accuracy premia a modelos conservadores que emiten pocas alertas (acertando en la clase mayoritaria), mientras que el PR-AUC evalúa la capacidad global de ordenar correctamente los casos positivos.

**14. En el gráfico de PR-AUC de train contra test, ¿qué runs quedan más lejos de la diagonal y qué indica eso?**
El run que queda más lejos de la diagonal es run2 (random_forest, max_depth=sin_limite), con train_pr_auc = 1.000 frente a test_pr_auc = 0.535. Esto indica un fuerte sobreajuste (overfitting), es decir, que el modelo memorizó los datos de entrenamiento pero perdió capacidad de generalizar sobre datos nuevos.

**15. ¿Qué ventaja tiene consultar los runs con mlflow.search_runs en lugar de mirarlos en la interfaz? Escribí el filter_string que usarías para traer sólo tus bosques con PR-AUC de test mayor a 0,55.**
Consultar con mlflow.search_runs permite filtrar, ordenar y procesar programáticamente cientos de experimentos como una tabla, automatizando la selección del mejor modelo sin revisión manual. El filtro sería:
filter_string="tags.student_id = 'alberto' AND params.model_type = 'random_forest' AND metrics.test_pr_auc > 0.55"

**16. En fraude, ¿qué cuesta más, una alerta falsa o un contracargo no detectado? Si el equipo de riesgo sólo puede revisar una cantidad fija de alertas por día, ¿cómo usarías el umbral?**
En fraude suele costar mucho más un contracargo no detectado (pérdida directa del dinero y penalizaciones) que una alerta falsa (costo operativo de revisión). Si el equipo tiene capacidad fija de revisión por día, ajustaría el umbral dinámicamente hacia arriba o hacia abajo hasta que la cantidad de transacciones que superen ese score coincida con el cupo diario de casos que pueden auditar.

## Registro, uso y monitoreo

**17. ¿Se pudo registrar el modelo en Unity Catalog? Indicá nombre, versión y alias (o el error, si falló). ¿Qué ventaja tiene cargar models:/...@champion en lugar de runs:/<run_id>/model?**
Sí, se registró exitosamente en Unity Catalog bajo el nombre workspace.bigdata_alberto.fraud_model, versión 1, con el alias champion. Cargar models:/...@champion desacopla el código productivo del entrenamiento: permite promover una nueva versión mejorada simplemente moviendo el alias, sin modificar ni redesplegar el pipeline que consume el modelo.

**18. ¿Cuántas transacciones nuevas se puntuaron y cuántas generaron alerta? ¿Qué columnas de ml_predictions permiten auditar cada decisión? ¿Por qué el umbral se guarda fuera del modelo?**
Se puntuaron 7,165 transacciones nuevas y 1,825 generaron alerta (25.5%). Las columnas threshold, model_name, model_version, run_id y scored_at permiten auditar exactamente qué versión, ejecución y criterio produjo cada alerta. El umbral se guarda fuera del modelo porque es una decisión de negocio (ajustable según capacidad operativa o tolerancia al riesgo) que puede modificarse sin necesidad de reentrenar.

**19. Cuando llegaron los contracargos, ¿el PR-AUC sobre nuevas fue parecido al de test? En la campaña simulada, ¿qué pasó con el porcentaje de alertas? ¿Qué es el drift y por qué se puede detectar antes de tener las etiquetas?**
Sí, el PR-AUC en producción (nuevas) fue de 0.579, muy parecido al 0.586 obtenido en test. En la campaña simulada, el porcentaje de alertas saltó del 25.5% al 42.9% (al subir el importe promedio y los pagos por transferencia). El drift es el cambio en la distribución de los datos respecto al entrenamiento y puede detectarse en tiempo real (antes de tener las etiquetas) monitoreando desviaciones en las variables de entrada y en la distribución de scores predichos.

## Cierre

**20. Ubicá lo que hiciste en el ciclo de vida de MLOps (datos, entrenamiento, tracking, registro, despliegue, monitoreo). ¿Qué faltaría para tener este modelo en producción real? Nombrá al menos dos cosas.**
En esta práctica recorrimos todo el ciclo de MLOps: preparación de datos (ml_features en 00), entrenamiento y tracking con parámetros/métricas (01 y 02), selección y registro con alias champion en Unity Catalog (03 y 04), despliegue batch (ml_predictions) y monitoreo de métricas y drift (04). Para llevarlo a producción real faltaría:

• Orquestación y reentrenamiento automatizado (CI/CD/CT) mediante Workflows/Jobs programados que disparen alertas y reentrenen al detectar drift.
• Inferencia en tiempo real (Model Serving) a través de una API REST de baja latencia para bloquear fraudes antes de aprobar la compra, en lugar de hacerlo solo en batch al día siguiente.
