# Calidad de datos: problemas, evidencia, acción y justificación

Fuente: Hotel Booking Demand (119.390 filas, 32 columnas, llegadas 2015-07-01 a 2017-08-31).
Las métricas salen de `scripts/profile_quality.py` (resultado en `docs/perfil_calidad.json`).
Las acciones se implementan en dbt: `brz_hotel_reservas` (tipado), `slv_reservas` (limpieza) y los tests.

## Completitud
| Problema | Evidencia | Acción | Justificación |
|---|---|---|---|
| `company` casi vacío | 112.593 nulos (94,3%) | No se conserva el ID; se crea `tiene_empresa` | Con 94% nulo el ID no aporta; saber si hay empresa sí es señal |
| `agent` vacío | 16.340 nulos (13,7%) | `agent_id` nulo y `tiene_agente` | Según la documentación del dataset, nulo = reserva sin intermediario |
| `country` vacío | 488 nulos (0,41%) | Se rellena con `UNK` | Categoría explícita; evita perder filas |
| `children` vacío | 4 nulos | Se rellena con 0 | Solo 4 filas; el resto de la reserva es válido |

## Consistencia
| Problema | Evidencia | Acción | Justificación |
|---|---|---|---|
| Duplicados exactos (la fuente no tiene ID) | 31.994 filas extra (26,8%); 119.390 -> 87.396 únicas | Se colapsan por `booking_key` (hash de las 32 columnas) y se guarda `n_filas_identicas` | No se puede distinguir una reserva repetida de un registro repetido; contarlas dos veces sesga el modelo (la tasa de cancelación pasa de 37,04% a 27,49%) |
| Reservas sin huéspedes | 180 en Raw (166 tras quitar duplicados) | Se excluyen de Silver | Una reserva sin adultos, niños ni bebés no es válida |
| `meal` con "Undefined" | 1.169 filas | Se mapea a `SC` | La documentación del dataset indica que son la misma categoría (sin plan) |
| Reservas con 0 noches | 715 en Raw (591 en Silver) | Se conservan y se marca `sin_noches` | Pueden ser cancelaciones tempranas o day-use; el modelo decide |
| Cliente repetido sin historial | 590 filas | Sin acción | No hay forma de saber cuál de los dos campos es el correcto |
| `reservation_status_date` no cuadra con llegada + noches | 27 de 75.166 check-outs (0,04%) | Sin acción | Impacto mínimo y la columna no pasa a Gold (ver Validez) |

## Precisión
| Problema | Evidencia | Acción | Justificación |
|---|---|---|---|
| Tarifa diaria (`adr`) imposible | 1 negativa (-6,38) y 1 de 5.400 | Se anula (NULL) y se marca `adr_invalido` | Imputar inventaría ingreso; NULL evita distorsionar `ingreso_estimado` |
| Tarifa diaria = 0 | 1.959 (1,64%); 680 son segmento Complementary | Se conserva y se marca `es_tarifa_cero` | En cortesías es legítimo; en el resto es ambiguo y se decide al modelar |
| Grupos muy grandes | 12 reservas con más de 10 adultos | Sin acción | Plausible en grupos; se revisará en el modelado |

## Validez
| Problema | Evidencia | Acción | Justificación |
|---|---|---|---|
| Fechas de llegada | 0 inválidas; rango 2015-07-01 a 2017-08-31 | Test `accepted_range` en Silver | Garantiza que ninguna carga futura traiga fechas fuera del dominio |
| Dominios de categorías | `is_canceled` solo 0/1; `hotel` solo 2 valores; `Undefined` en 2 (market_segment) y 5 (distribution_channel) | Tests `accepted_values`; `Undefined` se conserva como categoría | Son pocas filas y es información explícita de la fuente |
| **Fuga de información** | `reservation_status` coincide 100% con `is_canceled` (43.017 Canceled + 1.207 No-Show = 44.224) | `reservation_status`, `reservation_status_date` y `assigned_room_type` quedan en Silver (auditoría) y NO pasan a Gold | El modelo solo puede usar lo que se sabe al reservar (test `assert_sin_columnas_de_fuga_en_gold`) |
| Anomalía "Non Refund" | 14.587 filas Raw con 99,4% de cancelación; quedan 1.038 reservas únicas con 94,7% | No se corrige; test de alerta (`warn`) | Un depósito no reembolsable debería cancelar menos. El 93% de esas filas eran duplicados, pero la anomalía persiste |

## Limitaciones sin resolver
- La fuente no tiene ID de reserva: dos reservas distintas e idénticas se consideran una sola.
- El dataset es histórico (2015-2017) y de dos hoteles de Portugal.
- La moneda de `adr` no está documentada en las columnas.
