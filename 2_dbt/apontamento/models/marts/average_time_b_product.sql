{{ config(materialzed='table') }}

WITH orders_apontamento as (
    SELECT 
        order_id,
        order_date as start_order,
        quantidade as quantity, 
        termino   as end_order,
        duration
    FROM {{ ref('int_apontamento') }}
    WHERE termino IS NOT NULL
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
        a.duration
    FROM orders_apontamento a
    join orders_erp b
    on b.order_id = a.order_id
),
total_quantity_and_time_by_product as (
    SELECT 
        codigo,
        referencia,
        SUM(quantity) AS total_quantity,
        SUM(duration) AS times_total,
        SUM(duration) / SUM(quantity) * 60  as avg_time_minutes

    FROM orders_produced_today
    GROUP BY 
        codigo,
        referencia
)
SELECT * FROM total_quantity_and_time_by_product 
order by codigo