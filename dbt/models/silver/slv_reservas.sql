{{ config(materialized='table') }}

-- SILVER: datos limpios, una fila por reserva única. Cada regla está justificada en docs/calidad_datos.md.
with brz as (
    select * from {{ ref('brz_hotel_reservas') }}
),

-- Regla 1. Duplicados exactos: la fuente no tiene ID, así que filas con las 32 columnas iguales
-- (misma llave) se colapsan en una. Se conserva la carga más reciente y se guarda cuántas filas había.
dedup as (
    select
        *,
        count(*) over (partition by booking_key) as n_filas_identicas,
        row_number() over (partition by booking_key order by _loaded_at desc, _batch_id desc) as rn
    from brz
),

unicas as (
    select * from dedup where rn = 1
),

limpias as (
    select
        booking_key,
        hotel,
        is_canceled,
        lead_time,
        arrival_date,
        arrival_date_year  as arrival_year,
        arrival_month,
        arrival_date_day_of_month as arrival_day,
        stays_in_weekend_nights,
        stays_in_week_nights,
        stays_in_weekend_nights + stays_in_week_nights as noches_total,
        adults,
        coalesce(children, 0) as children,                         -- Regla 3: 4 nulos -> 0 (sin niños)
        babies,
        case when meal = 'Undefined' then 'SC' else meal end as meal,   -- Regla 5: misma categoría según la documentación
        coalesce(country, 'UNK') as country,                       -- Regla 3: país desconocido explícito
        market_segment,
        distribution_channel,
        is_repeated_guest,
        previous_cancellations,
        previous_bookings_not_canceled,
        reserved_room_type,
        assigned_room_type,
        booking_changes,
        deposit_type,
        agent as agent_id,
        (agent is not null)   as tiene_agente,                     -- Regla 3: null de agent = sin intermediario
        (company is not null) as tiene_empresa,                    -- Regla 3: company 94% nulo -> solo se conserva si existe
        days_in_waiting_list,
        customer_type,
        -- Regla 4: tarifa imposible (negativa o > adr_max) -> null y se marca; tarifa 0 se conserva y se marca
        case when adr < 0 or adr > {{ var('adr_max') }} then null else adr end as adr,
        coalesce(adr < 0 or adr > {{ var('adr_max') }}, false) as adr_invalido,
        coalesce(adr = 0, false) as es_tarifa_cero,
        (stays_in_weekend_nights + stays_in_week_nights = 0) as sin_noches,   -- Regla 6: se conserva y se marca
        required_car_parking_spaces,
        total_of_special_requests,
        reservation_status,
        reservation_status_date,
        n_filas_identicas,
        _batch_id,
        _loaded_at
    from unicas
    -- Regla 2: una reserva sin huéspedes no es una reserva válida -> se excluye
    where adults + coalesce(children, 0) + babies > 0
)

select * from limpias
