-- Regla de negocio: la tasa de cancelación global debe estar en un rango plausible (ver dbt_project.yml).
-- Si se sale del rango, o llegaron datos corruptos o cambió el negocio: hay que revisar antes de modelar.
select round(avg(is_canceled), 4) as tasa_cancelacion
from {{ ref('fct_reservas') }}
having avg(is_canceled) < {{ var('tasa_cancelacion_min') }}
    or avg(is_canceled) > {{ var('tasa_cancelacion_max') }}
