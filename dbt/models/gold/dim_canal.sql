select distinct
    {{ dbt_utils.generate_surrogate_key(['market_segment', 'distribution_channel']) }} as canal_key,
    market_segment    as segmento_mercado,
    distribution_channel as canal_distribucion
from {{ ref('slv_reservas') }}
