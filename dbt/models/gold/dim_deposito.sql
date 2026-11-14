select distinct
    {{ dbt_utils.generate_surrogate_key(['deposit_type']) }} as deposito_key,
    deposit_type as tipo_deposito
from {{ ref('slv_reservas') }}
