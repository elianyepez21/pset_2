# Diccionario de las tablas del pipeline

| Capa | Tabla | Grano | Filas esperadas |
|---|---|---|---|
| RAW | `RAW.HOTEL_BOOKINGS_RAW` | una fila por fila de la fuente (+ `_BATCH_ID`, `_SOURCE_FILE`, `_LOADED_AT`) | 119.390 |
| BRONZE | `BRONZE.BRZ_HOTEL_RESERVAS` | una fila por fila de RAW, tipada, con `booking_key` | 119.390 |
| SILVER | `SILVER.SLV_RESERVAS` | una fila por reserva única | 87.230 |
| GOLD | `GOLD.FCT_RESERVAS` | una fila por reserva única (`booking_key`) | 87.230 |
| GOLD | `GOLD.DIM_*` (8) | una fila por valor distinto | hotel 2, fecha 793, canal 26, país 178, tipo cliente 4, depósito 3, habitación 9, plan de comida 4 |
| OBT | `OBT.OBT_RESERVAS` | una fila por reserva única (igual que la tabla de hechos) | 87.230 |
| OBT | `OBT.OBT_VALIDACIONES` | una fila por chequeo y por corrida | 15 por corrida |

Los conteos esperados salen de replicar las reglas de Silver con pandas sobre el CSV original (`spark/test_obt_local.py`).

## Columnas que NO pasan al modelo (y por qué)
- `reservation_status`, `reservation_status_date`, `assigned_room_type`: revelan el resultado (fuga). Solo existen en Silver.
- `cambios_reserva`, `plazas_parking`, `solicitudes_especiales`: se conocen o cambian después de reservar. Están en Gold pero la OBT las excluye por defecto (`COLUMNAS_POSTERIORES` en `spark/build_obt.py`).

## Conjunto de entrenamiento y prueba
La OBT trae la columna `CONJUNTO`: `train` si la llegada es anterior a 2017-01-01 y `test` si es posterior.
