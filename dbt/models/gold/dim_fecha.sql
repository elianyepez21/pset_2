-- Calendario completo (un día por fila) para el rango esperado de la fuente.
with dias as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('fecha_min') ~ "' as date)",
        end_date="dateadd(day, 1, cast('" ~ var('fecha_max') ~ "' as date))"
    ) }}
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))        as fecha_key,
    date_day                                         as fecha,
    year(date_day)                                   as anio,
    quarter(date_day)                                as trimestre,
    month(date_day)                                  as mes,
    monthname(date_day)                              as mes_nombre,
    day(date_day)                                    as dia,
    weekiso(date_day)                                as semana_iso,
    dayofweekiso(date_day)                           as dia_semana,      -- 1 = lunes ... 7 = domingo
    dayofweekiso(date_day) >= 6                      as es_fin_de_semana,
    month(date_day) between 6 and 9                  as es_temporada_alta -- supuesto de negocio: junio a septiembre
from dias
