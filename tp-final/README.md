# Trabajo Práctico Final — Big Data y MLOps

- **Estudiante:** Alberto Mario González Rodríguez
- **Variante elegida:** Variante 1 — Investigación: caso de negocio y arquitectura de datos
- **Caso de estudio:** Plataforma Analítica y Streaming para la Billetera Virtual *PagoYa* (caso hipotético)
- **Fecha de entrega:** Octubre de 2026

## Objetivo del trabajo

Diseñar y fundamentar una arquitectura de datos moderna basada en el paradigma **Lakehouse** (Medallion Architecture) y procesamiento en tiempo real para **PagoYa**, una billetera virtual en rápida expansión. La propuesta busca resolver los cuellos de botella operativos del sistema transaccional actual (OLTP), habilitar la detección temprana de fraude en segundos, automatizar la conciliación con comercios y garantizar el cumplimiento regulatorio y de gobierno de datos.

## Contenido de la carpeta `tp-final/`

- `README.md`: Este archivo con la identificación, resumen ejecutivo, estructura de la entrega e instrucciones de revisión.
- `TP_Final_BigData_Alberto_Gonzalez.pdf`: Documento completo del trabajo práctico en formato PDF listo para lectura y evaluación.
- `TP_Final_BigData_Alberto_Gonzalez.docx`: Versión editable en Microsoft Word con el desarrollo completo y diagramas embebidos.

## Instrucciones para revisar el trabajo

1. Abrir `TP_Final_BigData_Alberto_Gonzalez.pdf` (o su equivalente `.docx`) para revisar las 6 secciones requeridas por la consigna:
   - **Sección 1 — Necesidad de negocio:** Contexto, áreas involucradas, dolor actual, objetivos, beneficios esperados y KPIs de éxito.
   - **Sección 2 — Datos:** Fuentes, volumetría estimada, frecuencia de llegada, calidad, sensibilidad (PII/PCI) y restricciones.
   - **Sección 3 — Arquitectura propuesta:** Flujo end-to-end desde la ingesta (CDC + Kafka + Auto Loader) hasta el consumo (Delta Lake, Databricks SQL, Power BI y alertas en tiempo real).
   - **Sección 4 — Modelado de datos:** Diseño por capas (Bronze, Silver, Gold) y esquema estrella detallado con claves, granularidad y resolución de preguntas de negocio.
   - **Sección 5 — Justificación técnica:** Fundamentación de decisiones en base a escala, latencia, costos, seguridad, operación y compromisos (*trade-offs*).
   - **Sección 6 — Gráficos y diagramas:** Diagrama de arquitectura, diagrama entidad-relación del modelo Gold y proyecciones de volumen y latencia.
2. Todos los valores cuantitativos de volumen, latencia y ahorro corresponden a **estimaciones técnicas** explícitamente señaladas para el caso hipotético.

## Referencias y fuentes utilizadas

- Material teórico y práctico de la cátedra *Herramientas de Procesamiento para Grandes Volúmenes de Datos (2026)* — Arquitecturas Lakehouse, Delta Lake, Structured Streaming, CDC y Gobierno de Datos.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly Media.
- Armbrust, M. et al. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR.
- Normativa de referencia del Banco Central de la República Argentina (BCRA) sobre Proveedores de Servicios de Pago (PSP) y protección de datos personales (Ley 25.326).
