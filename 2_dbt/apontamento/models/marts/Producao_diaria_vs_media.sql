WITH producao_ultimo_mes AS (
	SELECT
		CAST(order_date AS DATE) AS data,
		SUM(quantidade) AS quantidade,
		AVG(quantidade) AS media
	FROM {{ ref('int_apontamento') }} 
    WHERE 
	order_date >= CURRENT_DATE - INTERVAL '30 DAYS'
	GROUP BY CAST(order_date AS DATE)
	ORDER BY data
),
media_diaria AS (
	SELECT 
		ROUND(AVG(quantidade),0) AS media_montagem
	FROM producao_ultimo_mes

),
producao_dia AS (
    SELECT
        SUM(quantidade) AS quantidade_dia
    FROM {{ ref('int_apontamento') }}
    WHERE CAST(order_date AS DATE) = CURRENT_DATE        
),

producao_dia_vs_media AS (
	SELECT 
    pd.quantidade_dia,
    md.media_montagem
FROM producao_dia pd
CROSS JOIN media_diaria md
)
SELECT
	ROUND(quantidade_dia / media_montagem, 4) AS performace
FROM producao_dia_vs_media
