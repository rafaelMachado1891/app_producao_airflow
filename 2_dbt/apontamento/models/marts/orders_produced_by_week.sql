WITH orders_apontamento as (
    SELECT 
        order_id,
        order_date,
        quantidade,
        duration
    FROM {{ ref('int_apontamento') }}

),

orders_by_system as (
    SELECT
        order_id,
        codigo,
        referencia,
        quantidade        
    FROM {{ ref('int_orders') }}

),
orders_by_produced as (
    SELECT 
        A.order_date,
        A.order_id,
        B.codigo,
        B.referencia,
        A.quantidade,
        A.duration
    FROM orders_apontamento A
    JOIN orders_by_system B 
    ON A.order_id = B.order_id
),
orders_produced_by_week AS(
    SELECT 
        EXTRACT(YEAR FROM order_date) AS current_year,
        EXTRACT(WEEK FROM order_date) AS weekend_to_year,
        SUM(quantidade) as quatity       
        
    FROM orders_by_produced
    GROUP BY 
        EXTRACT(YEAR FROM order_date),
        EXTRACT(WEEK FROM order_date)
    ORDER BY 1,2 
)
SELECT * FROM orders_produced_by_week