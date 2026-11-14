-- Completitud de la carga: Bronze debe tener exactamente las mismas filas que RAW (nada se pierde al tipar).
select r.n_raw, b.n_bronze
from (select count(*) as n_raw from {{ source('raw', 'hotel_bookings_raw') }}) r
cross join (select count(*) as n_bronze from {{ ref('brz_hotel_reservas') }}) b
where r.n_raw <> b.n_bronze
