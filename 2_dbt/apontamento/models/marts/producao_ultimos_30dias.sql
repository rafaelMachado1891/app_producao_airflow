WITH vendas AS (

    SELECT
        CAST(order_date AS DATE) AS data,
        SUM(quantidade) AS quantidade
    FROM 
        {{ ref('int_apontamento') }} 
    WHERE 
    order_date >= CURRENT_DATE - INTERVAL '30 DAYS'
    GROUP BY CAST(order_date AS DATE)
    ORDER BY data
)
SELECT * FROM vendas