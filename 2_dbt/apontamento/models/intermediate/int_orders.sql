WITH int_orders as (
    SELECT 
     "NUMERO"         AS order_id,
     "CODIGO"         AS codigo,
     "REFERENCIA"     AS referencia,
     "LINHA"          AS linha,
     "GRUPO"          AS grupo,
     "QUANTIDADE"     AS quantidade,
     "DATA_EMISSAO"    AS order_date


    FROM {{ ref('stg_orders') }} 
)

SELECT * FROM int_orders