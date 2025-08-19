WITH orders_apontamento as (
    SELECT 
        order_id,
        order_date as start_order,
        quantidade as quantity, 
        termino   as end_order,
        duration
    FROM {{ ref('int_apontamento') }}
    WHERE order_date::DATE = CURRENT_DATE AND
    termino IS NOT NULL
),
orders_erp as (
    SELECT
        order_id,
        codigo,
        referencia,
        quantidade,
        linha
    FROM {{ ref('int_orders') }}
),
orders_produced_today as (
    SELECT
        a.order_id,
        b.codigo,
        b.referencia,
        a.quantity,
        a.start_order,
        a.end_order,
        round(a.duration * 60, 2)AS duration,
        round(a.quantity * c.avg_time_minutes , 2) AS prevision

    FROM orders_apontamento a
    join orders_erp b
    on b.order_id = a.order_id
    JOIN {{ ref('average_time_b_product') }} c
    ON c.codigo = b.codigo
)
SELECT 
COALESCE(SUM(quantity),0)  AS quantity
FROM orders_produced_today