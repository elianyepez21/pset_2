-- GRANO: una fila por reserva única (booking_key). Todas las columnas están disponibles al momento de
-- reservar salvo las marcadas como "posteriores" (se pueden ir actualizando después de reservar).
-- NO incluye reservation_status, reservation_status_date ni assigned_room_type: revelan el resultado (fuga).
select
    booking_key,
    {{ dbt_utils.generate_surrogate_key(['hotel']) }}                                   as hotel_key,
    to_number(to_char(arrival_date, 'YYYYMMDD'))                                       as fecha_llegada_key,
    {{ dbt_utils.generate_surrogate_key(['market_segment', 'distribution_channel']) }} as canal_key,
    {{ dbt_utils.generate_surrogate_key(['country']) }}                                as pais_key,
    {{ dbt_utils.generate_surrogate_key(['customer_type']) }}                          as tipo_cliente_key,
    {{ dbt_utils.generate_surrogate_key(['deposit_type']) }}                           as deposito_key,
    {{ dbt_utils.generate_surrogate_key(['reserved_room_type']) }}                     as habitacion_key,
    {{ dbt_utils.generate_surrogate_key(['meal']) }}                                   as plan_comida_key,
    agent_id,                                                                          -- dimensión degenerada
    -- Medidas conocidas al reservar
    lead_time,
    stays_in_weekend_nights                                                            as noches_fin_semana,
    stays_in_week_nights                                                               as noches_semana,
    noches_total,
    adults                                                                             as adultos,
    children                                                                           as ninos,
    babies                                                                             as bebes,
    adults + children + babies                                                         as huespedes_total,
    adr,
    adr * noches_total                                                                 as ingreso_estimado,
    is_repeated_guest                                                                  as es_cliente_repetido,
    previous_cancellations                                                             as cancelaciones_previas,
    previous_bookings_not_canceled                                                     as reservas_previas_no_canceladas,
    days_in_waiting_list                                                               as dias_lista_espera,
    tiene_agente,
    tiene_empresa,
    adr_invalido,
    es_tarifa_cero,
    sin_noches,
    n_filas_identicas,
    -- Medidas posteriores a la reserva (pueden filtrar el resultado; el OBT las excluye por defecto)
    booking_changes                                                                    as cambios_reserva,
    required_car_parking_spaces                                                        as plazas_parking,
    total_of_special_requests                                                          as solicitudes_especiales,
    -- Variable objetivo
    is_canceled
from {{ ref('slv_reservas') }}
