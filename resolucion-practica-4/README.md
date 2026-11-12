# Entrega práctica 4 — Streaming

- **Nombre:** Alberto Mario González Rodríguez
- **student_id:** alberto
- **Escala:** small


## Log y offsets

**1. En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?**
Después de leer 3 mensajes, en la cola quedaron 2 mensajes (['pago_4', 'pago_5']) y en el log siguieron los 5 mensajes. En una cola tradicional leer un mensaje lo borra, mientras que un log conserva los datos y el consumidor solo avanza su posición (offset).

**2. Después de la llegada 2, ¿cuántos mensajes había leído el consumidor antifraude y cuántos reportes? ¿El atraso de reportes afectó a antifraude? ¿Por qué?**
Después de la llegada 2, antifraude había leído 180 mensajes y reportes 0 mensajes. El atraso no afectó a antifraude porque cada consumidor mantiene su propio checkpoint independiente sin interferir con los demás (aislamiento).

**3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?**
Un offset es la posición que indica hasta qué mensaje o versión leyó un consumidor (apuntando a la próxima versión a procesar). En esta práctica, cada consumidor guarda su offset en su propio checkpoint.

**4. ¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?**
El replay leyó los 280 mensajes del topic desde la versión 0. La arquitectura Kappa propone usar un único código de streaming reprocesando el log desde el principio, a diferencia de Lambda que mantiene dos sistemas separados (batch y streaming) con la lógica duplicada.

## Ingesta y enriquecimiento

**5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?**
Bronze procesó 60, 120, 40, 40 y 20 filas respectivamente en las cinco llegadas, y 0 filas al reejecutar sin archivos nuevos. El checkpoint de Auto Loader recuerda qué archivos ya fueron leídos para evitar procesarlos dos veces.

**6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?**
En la llegada stream_002 aparecieron 20 registros con INVALID_AMOUNT y 20 con UNKNOWN_CUSTOMER. Bronze guarda el texto original sin descartar errores para conservar el dato crudo y poder reprocesarlo en el futuro si cambia el contrato o las reglas.

**7. ¿Qué columnas agregó el join con silver_customers y silver_products? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?**
El join agregó las columnas country (de silver_customers) y category (de silver_products). Según la teoría, es un join stream-tabla que enriquece cada evento del flujo continuo con tablas estáticas de referencia.

## Tiempo y ventanas

**8. ¿Qué diferencia hay entre event time y processing time? Usá late_ok como ejemplo.**
El event time es el momento en que ocurrió el evento, mientras que el processing time es cuando llega al sistema. Por ejemplo, late_ok ocurrió a las 12:03 (event time), pero recién se procesó en la llegada 2 (processing time).

**9. ¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?**
El watermark valió 11:54, 11:58, 12:16, 12:17 y 12:35 al terminar las llegadas 1 a 5 respectivamente. Se calcula restando el margen de tolerancia configurado (10 minutos) al máximo event time visto hasta ese momento.

**10. ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?**
Aparecieron en la llegada 3 (emitidas 4 ventanas). No se emitieron antes porque en las llegadas 1 y 2 el watermark (11:54 y 11:58) todavía no había superado el cierre de ninguna ventana.

**11. ¿Qué pasó con late_bad? ¿Por qué se aceptó late_ok y se descartó late_bad?**
El evento late_bad (12:02) fue descartado en la llegada 4 y no sumó en ninguna ventana. Se descartó porque su event time era anterior al watermark vigente (12:16), mientras que late_ok (12:03) se aceptó porque llegó cuando el watermark estaba en 11:54, dentro del margen permitido.

**12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.**
Tumbling corta el tiempo en intervalos fijos sin solaparse (ej. facturación total cada 5 minutos), hopping usa ventanas fijas que se solapan al avanzar (ej. promedio móvil de ventas de los últimos 10 minutos actualizado cada 5), y session agrupa actividad por entidad hasta un período de inactividad (ej. carrito de compras de un usuario hasta 5 minutos sin actividad).

**13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?**
En modo update se emitió 2 veces (llegada 1 con 40 compras / $6,000 y llegada 2 corregida con 60 compras / $8,400), mientras que en append se emitió 1 sola vez (llegada 3 con 60 compras / $8,400). Update tiene menor latencia pero obliga al consumidor a manejar actualizaciones duplicadas, mientras que append garantiza un resultado único y final a costa de esperar a que cierre la ventana.

**14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?**
Con un watermark de 1 minuto se gana menor latencia porque las ventanas cierran y emiten resultados mucho más rápido. A cambio, se pierde completitud en los datos, ya que se descartarán eventos tardíos que demoren más de 1 minuto en llegar (como late_ok).

## Garantías y fallas

**15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por event_id? ¿Por qué el productor puede enviar el mismo evento dos veces?**
Sin deduplicar quedaron 220 filas en el destino y deduplicando por event_id quedaron 200 filas. El productor puede enviar el mismo evento dos veces porque ante una falla o duda de red reintenta el envío para no perder datos (semántica at-least-once).

**16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?**
Al reiniciar con el mismo checkpoint el total se mantuvo en 200 filas sin reprocesar nada (garantía exactly-once), mientras que al perderlo con append se volvió a procesar todo duplicando el resultado a 400 filas (garantía at-least-once).

**17. ¿Por qué con MERGE el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?**
Con MERGE el resultado se mantuvo en 200 filas porque solo inserta eventos cuyo event_id no existe previamente en la tabla destino. Una escritura idempotente significa que ejecutar la misma operación múltiples veces produce exactamente el mismo resultado final que aplicarla una sola vez.

## CDC

**18. ¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un UPDATE genera dos eventos?**
El log generó 13 eventos de cambio de cuatro tipos: insert (6), update_preimage (3), update_postimage (3) y delete (1). Un UPDATE genera dos eventos para registrar tanto el estado anterior de la fila (update_preimage) como el nuevo valor actualizado (update_postimage).

**19. ¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?**
Sí, la tabla se reconstruyó de forma idéntica a la original quedándose con el último cambio por customer_id y descartando los eliminados. El log conserva el historial completo que la tabla actual pierde, como los estados intermedios (el paso del cliente 2 por CL antes de BR) y los registros borrados (la existencia y eliminación del cliente 3).

## Cierre

**20. Según la comparación de motores de la teoría, ¿cuándo elegirías Spark Structured Streaming, Flink o Kafka Streams? Mencioná una ventaja de cada uno.**
Elegiría Spark Structured Streaming para unificar batch y streaming en un Lakehouse aprovechando su integración nativa con Delta Lake. Usaría Flink cuando se necesita latencia de milisegundos gracias a su procesamiento real evento por evento. Optaría por Kafka Streams para microservicios ligeros sobre Kafka ya que es una librería embebida que no requiere mantener un clúster aparte.
