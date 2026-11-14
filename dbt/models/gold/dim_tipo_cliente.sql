select distinct
    {{ dbt_utils.generate_surrogate_key(['customer_type']) }} as tipo_cliente_key,
    customer_type as tipo_cliente
from {{ ref('slv_reservas') }}
