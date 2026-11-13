# Validación del laboratorio Aurora

Fecha: 13 de noviembre de 2026. Entorno: Windows 10 22H2, 12 procesadores lógicos, 7,87 GiB RAM, CPU. Python 3.12.13, SQLite 3.53.1, uv 0.11.19, asyncua 2.1.0, PyModbus 3.11.4, Paho 2.1.0, Mosquitto 2.0.22 y PostgreSQL 18.6. Semilla de simulación 101. PostgreSQL se ejecutó con binarios nativos de validación y el perfil Compose usa una imagen versionada de PostgreSQL 17.

## Repetir la validación

```powershell
uv sync --locked
uv run ruff check
uv run ruff format --check
uv run pytest -m 'not integration'
uv run pytest tests/test_acquisition.py -m integration
$env:TEST_MOSQUITTO = 'C:\ruta\mosquitto.exe'
$env:TEST_PG_BIN = 'C:\ruta\pgsql\bin'
uv run pytest -p no:cacheprovider -q -s
```

Los binarios deben estar instalados o extraídos con sus bibliotecas. Las pruebas levantan instancias dedicadas en localhost con credenciales sintéticas generadas. En Linux, usa rutas absolutas a `mosquitto` y al directorio devuelto por `pg_config --bindir`. El grupo de integración informa un salto si no recibe las rutas. `TEST_CHROME` y `TEST_SCREENSHOT` permiten repetir la captura de interfaz. `TEST_RESOURCE_USAGE=1` activa la medición opcional si `psutil` está disponible.

## Requisitos y evidencia

D01–D10 aprobados en la ejecución nativa. Pasaron 12 pruebas en 144,83 s. La prueba individual de la demo se repitió con la medición de commit y pasó en 33,46 s.

| Criterio | Prueba | Evidencia |
| --- | --- | --- |
| D01 | `test_real_protocols`, `test_clean_live_demo` | Seis variables de ambos servidores TCP, con unidades. Gateway, MQTT, PostgreSQL y API en ejecución. |
| D02 | `test_acquisition.py` | Ambos órdenes de palabras, escala, booleanos, null, no finitos, rango, timestamps y calidad. |
| D03 | `test_durable_mqtt_and_postgres` | Comparación de los 2.000 IDs deterministas de la outbox con los IDs de PostgreSQL. |
| D04 | `test_durable_mqtt_and_postgres` | Diez reenvíos, un evento lógico, conflicto de contenido y evento inválido registrados. |
| D05 | `test_durable_mqtt_and_postgres` | Detenciones independientes de Mosquitto, consumidor y PostgreSQL. Reinicio abrupto del proceso gateway con SQLite pendiente y verificación posterior de los IDs. |
| D06 | `test_durable_mqtt_and_postgres` | Consumidor termina con código 42 tras el commit. La fila existe, el evento sigue pendiente y el ACK se recupera tras reinicio. |
| D07 | `test_outbox_capacity_persists_and_records_coverage`, `test_gateway_pauses_and_records_lost_coverage` | Capacidad de seis eventos en prueba, pausa de ciclo, contabilidad y recuperación de estado. |
| D08 | `test_lot_boundary_and_broker_acl` | Lotes A y B asignados por tiempo de evento al entregar fuera de orden. PostgreSQL rechaza solapamientos. |
| D09 | `test_lot_boundary_and_broker_acl`, `test_endpoints_limited` | Mosquitto rechaza anónimo y publicación de ACK por gateway. Configuración rechaza endpoints externos al laboratorio. |
| D10 | `test_clean_live_demo` | Arranque desde datos nuevos, health check por HTTP, pausa y recuperación con IDs, consulta histórica por lote y CSV, desconexión y recuperación del simulador, y apagado ordenado del gateway. |

Se midió con intervalo de 0,6 s por ciclo de seis variables. Con 126 eventos a 10,29 eventos/s, la latencia de observación a commit dio p50 de 0,146 s y p95 de 0,317 s. El fixture activa `track_commit_timestamp=on` para consultar la hora de commit en PostgreSQL. La métrica del panel usa la hora de inserción. El ejercicio de interrupción recuperó 2.000 de 2.000 IDs en 45,83 s desde que se reinició la publicación.

En la medición de commit se observaron 13,0 % de un procesador lógico y 253,2 MiB RSS sumados, tomando para RSS el mayor de dos puntos separados por 12 s. Incluye gateway, simuladores, API, consumidor, broker y procesos PostgreSQL. La suma puede contar memoria compartida varias veces y excluye Chrome. La [captura real](evidence/aurora-live.png) muestra la interfaz con los servicios de prueba.

## Fallos y límites de la verificación

- La primera prueba de recuperación con conexión PostgreSQL por mensaje superó el límite de 120 s. La corrección reutiliza la conexión del consumidor, conserva una transacción por mensaje y reabre la conexión tras una caída. Se ajustó la ventana de reintento para evitar que los reenvíos inmediatos desplacen eventos pendientes.
- El perfil Compose pasó `config --quiet` con Compose v2.39.4 y se comprobó la disponibilidad de las etiquetas de Docker Hub fijadas. El arranque con Docker Engine, la variante WSL2/Linux y la ejecución de GitHub Actions quedan pendientes. La integración nativa en Windows ejercita los protocolos y servidores reales, con PostgreSQL 18.6.
