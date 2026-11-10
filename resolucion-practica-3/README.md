# Entrega práctica 3 — Bases de datos NoSQL

- **Nombre:** Alberto Mario González Rodríguez
- **student_id:** alberto
- **Escala:** small


## CAP y PACELC

**1. Con la red partida, ¿qué respondió la réplica C en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?**
En modo CP, la réplica C respondió con error (ERROR: no disponible al leer y ERROR: sin quórum al escribir), sacrificando la disponibilidad. En modo AP, devolvió el valor desactualizado 100 y aceptó escribir 50, sacrificando la consistencia.

**2. En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?**
Después de la reparación mediante last-write-wins, quedó un saldo final de 50 en las tres réplicas. Como resultado, se perdió la escritura concurrente anterior de saldo=80 realizada en el grupo AB.

**3. Según el gráfico de latencia, ¿cuál es la p99 con W=1 y con W=3? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)**
La latencia p99 es de 9.6 ms con W=1 y sube a 55.6 ms con W=3. A cambio de esta mayor latencia, se gana mayor consistencia en los datos replicados.

**4. Con cinco réplicas partidas en ABC | DE, ¿qué lado pudo seguir escribiendo en modo CP y por qué?**
Solo pudo seguir escribiendo el lado ABC. Esto ocurre porque es el único grupo que mantiene la mayoría (quórum) necesaria de al menos 3 nodos (5 // 2 + 1 = 3).

## Clave-valor

**5. ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?**
El GET en memoria fue ~1,571,990 veces más rápido que con Spark (0.0003 ms vs. 473 ms). Redis guarda los datos en memoria para responder consultas puntuales en milisegundos (O(1)) sin el costo de leer disco.

**6. ¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.**
Hubo que parsear los 5,000 valores porque para la base el valor es un texto opaco y no conoce sus campos internos. Una ventaja es su altísima velocidad O(1) para búsquedas por clave y fácil particionado; una desventaja es que no permite filtrar por atributos internos del valor sin leer todo o mantener estructuras extra a mano.

**7. Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con hash mod N y con hashing consistente? ¿Por qué conviene el segundo?**
Con hash mod N se movió el 79.6% de las claves, mientras que con hashing consistente (usando 100 nodos virtuales) solo se movió el 21.2%. Conviene el hashing consistente porque al agregar un nodo evita reubicar la gran mayoría de los datos por la red, moviendo únicamente las claves del tramo ocupado.

## Documental

**8. ¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?**
La consulta relacional devolvió 11 filas (usando 3 tablas y 2 joins) y la documental devolvió 1 solo documento (0 joins). Dentro del documento quedaron embebidas sus transacciones como un array y una copia (snapshot) del producto dentro de cada transacción.

**9.  Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?**
Spark construyó un esquema con la unión de todos los campos y completó con null los que faltaban en cada documento. Se dice que tiene esquema flexible porque los documentos de una misma colección no necesitan tener exactamente los mismos campos ni una estructura rígida predefinida.

**10. ¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?**
El documento más grande pesa 5.2 KB (aproximadamente 225 bytes por transacción). Si un documento crece sin límite, puede alcanzar el tope máximo permitido por el motor (como los 16 MB en MongoDB) y degradar el rendimiento de lectura y escritura.

**11. Para calcular el monto por categoría hubo que usar explode. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?**
Resuelve muy bien las consultas centradas en una entidad principal (como traer todo el historial o filtrar arrays de un cliente sin hacer joins). En cambio, le cuestan más las consultas analíticas transversales que cruzan o agrupan datos entre múltiples documentos, obligando a desarmar arrays y volver a filas.

## Grafos

**12. ¿Cuántos nodos Customer, nodos Device y aristas USES tiene el grafo?**
El grafo tiene 5,000 nodos Customer (de los cuales 738 están marcados), 2,361 nodos Device y 5,400 aristas USES.

**13. ¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?**
SQL necesitó 3 joins para el patrón de 2 saltos y 5 joins para el de 4 saltos. [Neo4j] es más eficiente porque guarda punteros directos a sus vecinos en cada nodo (index-free adjacency), evitando calcular joins costosos en cada salto.

**14. ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?**
La propagación de etiquetas tardó 14 iteraciones en converger (llegando a 0 cambios). El componente conexo más grande (c:122) tiene 23 nodos.

**15. ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con hash(id) mod 4? ¿Por qué es difícil distribuir una base de grafos?**
Al repartir con hash(id) mod 4, el 74.4% de las aristas quedó cortado entre máquinas distintas. Distribuir grafos es difícil porque sus nodos están muy interconectados y separar vecinos obliga a realizar saltos costosos por la red para recorrer las relaciones.

## Vectorial

**16. ¿Qué representa el vector de cada cliente y qué mide la similitud coseno?**
El vector de 10 números resume el comportamiento de compra del cliente (gasto por categoría, uso de canales de pago, ticket promedio y proporción de compras altas). La similitud coseno mide qué tan parecida es la dirección entre dos vectores, donde 1 significa exactamente la misma dirección.

**17. ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?**
Entre los vecinos de clientes marcados hay un 31.6% de marcados, frente a una tasa base general del 14.8%. Buscar "vecinos parecidos" sirve para clasificar entidades o detectar comportamientos similares (como riesgo de fraude o recomendaciones) a partir de su cercanía en el espacio vectorial.

**18. En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con nprobe=4? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?**
Con nprobe=4 se obtiene un recall@10 de 0.872 escaneando solo el 5.985% de los vectores. Con la búsqueda aproximada (ANN) se gana velocidad al revisar menos datos, pero se pierde algo de exactitud al poder omitir algún vecino verdadero.

## Columnar

**19. ¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de amount sobre Parquet (ReadSchema)? ¿Cuántos archivos se pueden saltear con los datos ordenados?**
Los datos ocupan 50 MB en CSV, 121.1 MB en JSON y 5.3 MB en Parquet. Para la consulta sobre Parquet, Spark leyó únicamente la columna amount (ReadSchema: struct<amount:decimal(12,2)>), y al estar los datos ordenados se pueden saltear 7 de 8 archivos.

**20. ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?**
Es bueno para analítica porque lee solo las columnas necesarias (column pruning), comprime mejor datos homogéneos y saltea bloques mediante estadísticas min/max. En cambio, es poco conveniente para modificar una fila por vez porque los campos de cada registro están guardados en lugares separados del archivo.
