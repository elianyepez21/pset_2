-- Trazabilidad: filas de Bronze = filas colapsadas en Silver + filas excluidas (duplicados y reservas sin huéspedes).
-- Silver solo puede tener menos filas que Bronze, nunca más, y sum(n_filas_identicas) no puede superar a Bronze.
select s.n_silver, s.n_colapsadas, b.n_bronze
from (select count(*) as n_silver, sum(n_filas_identicas) as n_colapsadas from {{ ref('slv_reservas') }}) s
cross join (select count(*) as n_bronze from {{ ref('brz_hotel_reservas') }}) b
where s.n_silver > b.n_bronze or s.n_colapsadas > b.n_bronze
