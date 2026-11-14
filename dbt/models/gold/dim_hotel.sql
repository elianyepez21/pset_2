select distinct
    {{ dbt_utils.generate_surrogate_key(['hotel']) }} as hotel_key,
    hotel as hotel_nombre
from {{ ref('slv_reservas') }}
