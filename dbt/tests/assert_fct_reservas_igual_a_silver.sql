-- El grano de la tabla de hechos es la reserva única: debe tener las mismas filas que Silver (ni más ni menos).
select s.n_silver, f.n_fct
from (select count(*) as n_silver from {{ ref('slv_reservas') }}) s
cross join (select count(*) as n_fct from {{ ref('fct_reservas') }}) f
where s.n_silver <> f.n_fct
