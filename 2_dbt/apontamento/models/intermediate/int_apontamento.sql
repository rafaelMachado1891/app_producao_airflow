with int_apontamento as (
    SELECT 
        ORDER_ID      as order_id,
        ORDER_DATE    as order_date,
        QUANTIDADE    as quantidade,
        TERMINO       as termino,
        TEMPO_TOTAL_MINUTOS as duration
    FROM {{ ref('stg_apontamento') }}
    WHERE ORDER_DATE::DATE <> CURRENT_DATE
      AND TERMINO IS NOT NULL
),
ordens_erp as (
    SELECT 
        "NUMERO"
    FROM {{ ref('stg_orders') }}
),
ordens_agregadas as (
    SELECT 
        order_id,
        order_date,
        quantidade,
        termino,
        duration
    FROM int_apontamento
    WHERE order_id IN (SELECT "NUMERO" FROM ordens_erp)
)
select * from ordens_agregadas