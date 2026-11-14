-- Consistencia: ingreso_estimado debe ser exactamente tarifa diaria x noches.
select booking_key, adr, noches_total, ingreso_estimado
from {{ ref('fct_reservas') }}
where adr is not null
  and abs(ingreso_estimado - adr * noches_total) > 0.01
