# Procedencia y licencias

Los datos de planta Aurora son sintéticos. El código MIT genera las series a partir de ciclos, semilla y mapas definidos para este proyecto. No hay conjuntos de datos, pesos de modelos, manuales ni planos de terceros redistribuidos.

| Dependencia o servicio | Fuente y licencia |
| --- | --- |
| asyncua | [FreeOpcUa, LGPL-3.0](https://github.com/FreeOpcUa/opcua-asyncio/blob/master/LICENSE) |
| PyModbus | [PyModbus, BSD-3-Clause](https://github.com/pymodbus-dev/pymodbus/blob/v3.11.4/LICENSE) |
| Paho MQTT Python | [Eclipse, EPL-2.0 o EDL-1.0](https://github.com/eclipse-paho/paho.mqtt.python/blob/v2.1.0/LICENSE.txt) |
| Mosquitto | [Eclipse, EPL-2.0 o EDL-1.0](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/LICENSE.txt) |
| Psycopg | [Psycopg, LGPL-3.0](https://www.psycopg.org/psycopg3/docs/) |
| PostgreSQL | [PostgreSQL License](https://www.postgresql.org/about/licence/) |
| FastAPI, Pydantic | [FastAPI, MIT](https://github.com/fastapi/fastapi/blob/master/LICENSE), [Pydantic, MIT](https://github.com/pydantic/pydantic/blob/main/LICENSE) |
| Uvicorn | [Uvicorn, BSD-3-Clause](https://github.com/encode/uvicorn/blob/master/LICENSE.md) |

`uv.lock` fija versiones y hashes. Las distribuciones instaladas conservan sus avisos bajo `*.dist-info`. Los binarios de validación de Mosquitto y PostgreSQL se obtuvieron de [MSYS2](https://packages.msys2.org/packages/mingw-w64-ucrt-x86_64-mosquitto) y [embedded-postgres](https://github.com/kainpl/embedded-postgres) y quedaron fuera del historial y del respaldo de fuentes.
