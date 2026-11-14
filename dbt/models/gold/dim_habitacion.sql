select distinct
    {{ dbt_utils.generate_surrogate_key(['reserved_room_type']) }} as habitacion_key,
    reserved_room_type as tipo_habitacion_reservada
from {{ ref('slv_reservas') }}
