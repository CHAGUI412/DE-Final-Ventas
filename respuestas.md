# Examen final - Ventas (dbt + Airflow, source freshness)

## Respuestas conceptuales

### 1. ¿Por qué usar `_loaded_at` y no `fecha_venta` para este control?

Porque cada una mide algo distinto. `fecha_venta` dice cuándo ocurrió la venta en el negocio, y una venta vieja puede llegar hoy sin problema (por ejemplo una carga atrasada de ventas de hace una semana). Si el control usara `fecha_venta`, una carga que acaba de llegar se vería como "vieja" y daría error aunque los datos estén al día. Al revés, si la carga se detiene, la última `fecha_venta` seguiría pareciendo reciente por un tiempo. `_loaded_at` dice cuándo llegó el dato al sistema, que es justo lo que queremos saber: si la carga se retrasó. Por eso freshness toma el máximo de `_loaded_at` y lo compara con la hora actual en UTC.

### 2. ¿Por qué las pruebas de integridad no garantizan actualidad?

Las pruebas de unicidad, campos obligatorios y valores válidos solo revisan que los datos que ya están en la tabla sean consistentes. No miran cuándo llegaron. Una tabla con datos de hace 30 horas puede tener ids únicos, ningún nulo y valores correctos, y pasar todas las pruebas igual. En el escenario desactualizado se ve: el resumen sigue teniendo las mismas tres filas y las pruebas pasarían, pero la fuente está vieja. Además `dbt build` no ejecuta freshness por su cuenta, entonces hay que pedirla aparte (`dbt source freshness`) y poner esa tarea antes del build en el DAG, para que un error de freshness deje el build en `upstream_failed` y se conserve el resumen anterior.

## Archivos modificados

- `dbt/models/ventas_diarias.sql`
- `dbt/models/sources.yml`
- `airflow/dags/examen_ventas.py`

## Evidencias (carpeta `evidencias/`)

Escenario 1, fuente actualizada. Run ID `manual__2026-11-12T11:38:27.685114+00:00`, freshness y construir_resumen en `success`, freshness antes de build:

- `01a_escenario1_detalles.png`
- `01b_escenario1_grafo.png`
- `02a_escenario1_log_freshness.png`
- `02b_escenario1_log_construir_resumen.png`

Escenario 2, fuente desactualizada. Run ID `manual__2026-11-12T11:48:55.872877+00:00`, freshness en `failed` y construir_resumen en `upstream_failed` sin ejecutarse:

- `03a_escenario2_grafo.png`
- `03b_escenario2_detalles.png`
- `03c_escenario2_log_freshness.png`
- `04_log_freshness_error.txt`: salida de `dbt source freshness` con `ERROR STALE` de `ventas_raw.ventas` (el log de Airflow solo muestra INICIO/FIN dbt y la excepción, no el motivo).
- `05_resumen_despues_del_bloqueo.txt`: `analytics.ventas_diarias` conserva las mismas 3 filas.

Resumen consultado antes del bloqueo: la consulta `select * from analytics.ventas_diarias order by fecha_venta` se ejecutó después del build del escenario 1 y devolvió las mismas 3 filas (2026-01-10: 2, 3, 46.00; 2026-01-11: 3, 6, 51.25; 2026-01-12: 1, 4, 12.40). Esa salida no quedó guardada como archivo; aparece en el hilo del chat adjunto a la entrega, con su hora, y coincide con el resumen posterior.

Uso de IA: el desarrollo se hizo con Claude; el hilo completo se adjunta al profesor.
