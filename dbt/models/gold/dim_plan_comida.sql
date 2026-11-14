select distinct
    {{ dbt_utils.generate_surrogate_key(['meal']) }} as plan_comida_key,
    meal as plan_comida
from {{ ref('slv_reservas') }}
