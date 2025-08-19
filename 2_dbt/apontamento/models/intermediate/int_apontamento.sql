with int_apontamento as (
    SELECT 
        order_id        as order_id,
        order_date      as order_date,
        QUANTIDADE      as quantidade,
        TERMINO         as termino,
        TEMPO_TOTAL_MINUTOS as duration

    FROM {{ ref('stg_apontamento') }}

)

select * from int_apontamento