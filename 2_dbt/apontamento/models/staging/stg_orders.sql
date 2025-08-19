
with deduplicado as (
select 
    *,
    row_number() over (partition by "NUMERO" order by "DATA_EMISSAO" desc) as rn
from {{ source('apontamento', 'ORDERS') }}
)

select 
    "NUMERO",
    "CODIGO",
    "REFERENCIA",
    "LINHA",
    "GRUPO",
    "QUANTIDADE",
    "SALDO",
    "DATA_EMISSAO",
    "DATA_ENTREGA"
from deduplicado
where rn = 1