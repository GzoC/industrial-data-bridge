# Guía técnica y de demostración

## Mapa de adquisición

| Variable | Protocolo y referencia | Tipo y rango |
| --- | --- | --- |
| TK101.level | OPC UA, `s=TK101.level` | Double, 0–2 m |
| TK101.temperature | OPC UA, `s=TK101.temperature` | Double, 15–40 °C |
| P101.flow | Modbus holding 0 y 1 | float32, 0–20 L/min |
| P101.pressure | Modbus holding 2 y 3 | float32, 0–6 bar |
| P101.speed | Modbus holding 4 | uint16, 0–3000 rpm |
| P101.running | Modbus coil 0 | booleano |

La URI OPC UA `urn:aurora:sim:tk101` es estable. El cliente obtiene su índice al conectar. En Modbus la dirección cero equivale a la referencia humana 40001 para holding registers. El dispositivo predeterminado es 1. Se usan bytes en big endian dentro de cada palabra y orden de palabras configurable. La adquisición predeterminada se programa a 1 Hz por variable. Una lectura lenta puede reducir la frecuencia observada.

## Contratos y trazabilidad

El esquema ejecutable está en `src/bridge/events.py`. Este ejemplo corresponde al mensaje publicado en `aurora/v1/aurora-gw/events`.

```json
{
  "event_id": "d2091448-6d69-4ba2-9b45-2f1027584662",
  "schema_version": 1,
  "gateway_id": "aurora-gw",
  "session_id": "900482af-a073-414e-b5e6-25959a4ca299",
  "sequence": 1,
  "site_id": "aurora-demo",
  "asset_id": "P101",
  "tag_id": "P101.flow",
  "source_protocol": "modbus",
  "source_ref": "holding:0-1",
  "source_timestamp": null,
  "observed_at": "2026-11-13T12:00:00Z",
  "received_at": null,
  "timestamp_origin": "gateway",
  "raw_value": [16704, 0],
  "value": 12.0,
  "unit": "L/min",
  "quality": "GOOD",
  "quality_reasons": [],
  "source_status": null,
  "simulated": true
}
```

`observed_at` lo asigna el gateway al completar la lectura. `source_timestamp` y `source_status` vienen de OPC UA cuando están presentes. Un reloj de origen separado más de diez segundos del gateway degrada la calidad y usa el tiempo del gateway. Modbus usa siempre `observed_at`. El consumidor guarda `received_at` aparte del cuerpo original, antes de entrar a la transacción. PostgreSQL asigna `write_at` durante la inserción. El ACK se emite después del commit. La secuencia empieza en 1 por sesión UUID y distingue épocas tras un reinicio.

El consumidor rechaza versiones desconocidas, identificadores fuera del mapa, unidades incompatibles y números JSON no finitos. Una lectura con fallo de enlace o fuera de rango conserva `value: null`, calidad BAD y el motivo. Si la fuente entrega un flotante no finito, el diagnóstico conserva el patrón Modbus o el nombre de ese valor OPC UA en `raw_value`. Un valor congelado con enlace activo mantiene la calidad comunicada por el protocolo.

El ACK v1 usa `schema_version`, `event_id`, `digest` (SHA-256 del JSON normalizado) y `outcome` (`stored` o `conflict`). Una retransmisión idéntica puede recibir otra confirmación `stored`. Un conflicto se guarda en PostgreSQL y se registra como terminal en la outbox, sin sobrescribir el original. Sólo el tópico de estado utiliza mensajes retenidos y LWT. Las lecturas y los ACK se publican sin retención. El reintento tiene espera exponencial con variación y límite por intento. Persiste mientras el evento siga pendiente. Al llenarse los 10.000 espacios, el gateway pausa el siguiente ciclo completo y contabiliza las lecturas omitidas. Una caída de equipo genera huecos físicos irrecuperables.

Las órdenes usan intervalos UTC `[valid_from, valid_to)` con exclusión de solapamientos por activo. El consumidor busca el lote por `event_time` y guarda una copia del contexto junto al evento. Una entrega tardía conserva el lote histórico. Si falta orden, el contexto queda nulo y la regla indica el origen temporal. La API permite ingresar intervalos nuevos. Una corrección histórica futura debería ser explícita y auditable, conservando el contexto con que se recibió el evento. La vista actual se ordena por tiempo del evento, con desempate por observación e ID.

## Demostración de 3 a 5 minutos

1. Arranca el laboratorio con los comandos del README. Abre la interfaz y verifica seis variables con unidades, fuente, calidad y antigüedad. Crea las órdenes con **Crear órdenes sintéticas**.
2. Ejecuta `docker compose stop broker`. Espera a que suba el pendiente, pulsa **Guardar IDs pendientes** y ejecuta `docker compose start broker`. Pulsa **Verificar IDs en PostgreSQL** después de recuperar el enlace. Debes ver los IDs almacenados y bajar el backlog.
3. Pausa y reanuda el consumidor desde el panel. El flujo se recupera aunque el broker se haya mantenido activo. Para provocar la pérdida del ACK después de un commit, ejecuta `docker compose exec consumer touch /run/bridge-controls/consumer-crash-after-commit`, revisa que el consumidor salga y levántalo con `docker compose start consumer`. Observa los reenvíos en el contador.
4. Cambia OPC UA a **Desconectado**. Las lecturas BAD aparecen sin valor y se corta la línea del histórico. Selecciona **Normal**, consulta el histórico y descarga el CSV.
5. Consulta `/docs` para la API y `/api/stats` para latencias, tasas y contadores. Detén el entorno con `docker compose down`.

## Cómo defender el proyecto

- Explica la diferencia entre confirmación de MQTT, commit de PostgreSQL y ACK de aplicación. Puedes mostrar el mismo ID en la outbox y en la fila confirmada.
- Explica por qué el tiempo de recepción no sirve para asignar un lote. Dibuja dos intervalos consecutivos y prueba una medición tardía inmediatamente anterior a la frontera.
- Explica cómo verificarías la escala y el orden de palabras contra el mapa autorizado de un equipo, y por qué un valor estable con comunicaciones activas no prueba un enlace caído.
- El trabajo es aplicable a integración de históricos y a trazabilidad de producción. Un piloto requiere validar relojes, permisos, mapa de equipos, certificados, retención, respaldo y límites de carga en condiciones de planta. Aquí los protocolos de campo están confinados al laboratorio y funcionan sin TLS. Sólo la API se publica, en localhost.
