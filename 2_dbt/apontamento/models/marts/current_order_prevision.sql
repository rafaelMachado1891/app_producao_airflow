with orders_apontamento as (
    select 
        order_id,
        order_date
    from {{ ref('int_apontamento') }}
    where order_date::DATE = CURRENT_DATE AND
    termino IS NULL

),
orders_erp as (
    select
        order_id,
        referencia,
        linha,
        quantidade
    from {{ ref('int_orders') }}
),
tempo_medio_producao AS (
    SELECT 
        referencia,
        avg_time_minutes
    
    FROM {{ ref('average_time_b_product') }}
       
),
order_in_process as (
    select
        a.order_id,
        b.referencia,
        b.quantidade,
        a.order_date,
        C.avg_time_minutes * b.quantidade as prevision_time_minutes  
        
    from orders_apontamento a 
    join orders_erp b 
    on a.order_id = b.order_id
    JOIN tempo_medio_producao c
    on c.referencia = b.referencia
)
select * from order_in_process