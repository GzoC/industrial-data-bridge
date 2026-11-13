# Continuación del proyecto

## Evolución configurable

Rama `codex/configurable-acquisition`, basada en `9c4ccbe`. Leer
`VALIDACION.md` para separar pruebas nativas de las que dependen de CI.
Verificar el trabajo `integration` y repetir con certificados de laboratorio
para OPC UA y MQTT. Usar el perfil alternativo descrito en el README.

Fecha de inicio: 13 de noviembre de 2026. Encargo: puente de datos industriales OT/IT.

## Hitos de trabajo

1. Probar Python, dependencias y servidores reales de OPC UA y Modbus TCP. Verificar el mapa de tags y la normalización.
2. Persistir en SQLite antes de publicar por MQTT. Verificar el commit en PostgreSQL, el ACK de aplicación y los reenvíos.
3. Incorporar órdenes, consultas, controles de laboratorio y evidencia de recuperación en la interfaz.
4. Ejecutar D01–D10 y el recorrido de demostración. Registrar comandos, resultados, evidencia y limitaciones.

## Control de cuota de Codex

Consultar el porcentaje restante de la ventana de 300 minutos en cada hito y antes de pruebas largas. Con 5 % o menos, cerrar la implementación, guardar los archivos y el estado de Git, crear una copia local verificable con los cambios sin commit y anotar el próximo comando. Excluir secretos, cachés y dependencias descargadas. Reanudar cuando haya cuota disponible y se solicite continuar.

Primera lectura: 89 % restante. El directorio IT padre estaba vacío y sin commits ni remoto. Se inicializó el proyecto en un repositorio propio en la rama `codex/industrial-data-bridge`.

## Punto de continuación

El laboratorio incluye simuladores, gateway, outbox, broker con ACL, consumidor, PostgreSQL, API, interfaz, pruebas y documentación. Los comandos de arranque están en el README. Las evidencias y comandos ejecutados están en `VALIDACION.md`. El siguiente paso de plataforma es repetir `docker compose up --build -d` y la guía con Docker Engine o WSL2, y ejecutar la CI cuando exista un remoto autorizado. El respaldo local incluye el código, el historial de Git y el encargo original. Los datos de las pruebas se vuelven a generar.
