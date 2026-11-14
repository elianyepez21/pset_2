{{ config(severity='warn') }}
-- ALERTA DE NEGOCIO (no bloquea el pipeline): la política "Non Refund" debería cancelar MENOS que el resto,
-- pero en esta fuente cancela más del 90%. Es una anomalía conocida del dataset; hay que tenerla presente
-- antes de proponer depósitos como acción (ver docs/calidad_datos.md, limitaciones).
select
    d.tipo_deposito,
    round(avg(f.is_canceled), 4) as tasa_cancelacion,
    count(*) as reservas
from {{ ref('fct_reservas') }} f
join {{ ref('dim_deposito') }} d on f.deposito_key = d.deposito_key
where d.tipo_deposito = 'Non Refund'
group by d.tipo_deposito
having avg(f.is_canceled) > 0.90
