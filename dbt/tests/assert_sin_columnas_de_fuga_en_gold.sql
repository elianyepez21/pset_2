-- Regla del problema de negocio: el modelo solo puede usar información disponible al reservar.
-- Estas columnas revelan si la reserva se canceló, así que no deben existir en la tabla de hechos.
select column_name
from {{ target.database }}.information_schema.columns
where table_schema = 'GOLD'
  and table_name = 'FCT_RESERVAS'
  and lower(column_name) in ('reservation_status', 'reservation_status_date', 'assigned_room_type')
