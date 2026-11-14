select distinct
    {{ dbt_utils.generate_surrogate_key(['country']) }} as pais_key,
    country as pais_codigo
from {{ ref('slv_reservas') }}
