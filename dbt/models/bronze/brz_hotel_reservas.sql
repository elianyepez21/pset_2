{{ config(materialized='table') }}

-- BRONZE (staging): una fila por fila de RAW. Solo tipado, recorte de espacios y llave técnica.
-- No se elimina ni corrige nada aquí: la limpieza ocurre en Silver.
with raw as (
    select * from {{ source('raw', 'hotel_bookings_raw') }}
),

con_llave as (
    select
        raw.*,
        -- La fuente no trae ID de reserva: la llave es el hash de las 32 columnas de negocio.
        -- Filas idénticas obtienen la misma llave (así se detectan los duplicados en Silver).
        {{ dbt_utils.generate_surrogate_key([
            'hotel',
            'is_canceled',
            'lead_time',
            'arrival_date_year',
            'arrival_date_month',
            'arrival_date_week_number',
            'arrival_date_day_of_month',
            'stays_in_weekend_nights',
            'stays_in_week_nights',
            'adults',
            'children',
            'babies',
            'meal',
            'country',
            'market_segment',
            'distribution_channel',
            'is_repeated_guest',
            'previous_cancellations',
            'previous_bookings_not_canceled',
            'reserved_room_type',
            'assigned_room_type',
            'booking_changes',
            'deposit_type',
            'agent',
            'company',
            'days_in_waiting_list',
            'customer_type',
            'adr',
            'required_car_parking_spaces',
            'total_of_special_requests',
            'reservation_status',
            'reservation_status_date'
        ]) }} as booking_key
    from raw
),

tipado as (
    select
        booking_key,
        trim(hotel) as hotel,
    try_to_number(is_canceled) as is_canceled,
    try_to_number(lead_time) as lead_time,
    try_to_number(arrival_date_year) as arrival_date_year,
    {{ month_to_number('arrival_date_month') }} as arrival_month,
    try_to_number(arrival_date_week_number) as arrival_date_week_number,
    try_to_number(arrival_date_day_of_month) as arrival_date_day_of_month,
    try_to_number(stays_in_weekend_nights) as stays_in_weekend_nights,
    try_to_number(stays_in_week_nights) as stays_in_week_nights,
    try_to_number(adults) as adults,
    try_to_number(children) as children,
    try_to_number(babies) as babies,
    trim(meal) as meal,
    trim(country) as country,
    trim(market_segment) as market_segment,
    trim(distribution_channel) as distribution_channel,
    try_to_number(is_repeated_guest) as is_repeated_guest,
    try_to_number(previous_cancellations) as previous_cancellations,
    try_to_number(previous_bookings_not_canceled) as previous_bookings_not_canceled,
    trim(reserved_room_type) as reserved_room_type,
    trim(assigned_room_type) as assigned_room_type,
    try_to_number(booking_changes) as booking_changes,
    trim(deposit_type) as deposit_type,
    try_to_number(agent) as agent,
    try_to_number(company) as company,
    try_to_number(days_in_waiting_list) as days_in_waiting_list,
    trim(customer_type) as customer_type,
    try_to_decimal(adr, 10, 2) as adr,
    try_to_number(required_car_parking_spaces) as required_car_parking_spaces,
    try_to_number(total_of_special_requests) as total_of_special_requests,
    trim(reservation_status) as reservation_status,
        _batch_id,
        _source_file,
        _loaded_at
    from con_llave
)

select
    *,
    try_to_date(lpad(arrival_date_year::varchar, 4, '0') || '-' || lpad(arrival_month::varchar, 2, '0')
                || '-' || lpad(arrival_date_day_of_month::varchar, 2, '0')) as arrival_date
from tipado
