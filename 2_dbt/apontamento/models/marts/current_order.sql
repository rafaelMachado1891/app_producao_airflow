with orders_apontamento as (
    select 
        order_id,
        order_date
    from {{ ref('int_apontamento') }}

),
orders_erp as (
    select
        order_id,
        referencia,
        linha,
        quantidade
    from {{ ref('int_orders') }}
),
registro_atual as (
    select 
        max(order_date) as ultimo_registro
    from orders_apontamento
),
ultima_order as (
    select
        order_id,
        max(order_date) as ultimo_registro
    from orders_apontamento
    where 
        order_date::DATE = CURRENT_DATE
    group by 
        order_id
),
order_in_process as (
    select
        a.order_id,
        b.ultimo_registro
    from ultima_order a 
    join registro_atual b 
    on a.ultimo_registro = b.ultimo_registro
),
dados_orders as (
    select 
        a.order_id,
        b.referencia,
        b.quantidade,
        a.ultimo_registro as inicio
    from order_in_process a 
    join 
    orders_erp b 
    on b.order_id = a.order_id
)

select * from dados_orders