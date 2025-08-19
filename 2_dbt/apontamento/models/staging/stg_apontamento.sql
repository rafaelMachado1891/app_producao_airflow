WITH dados_brutos AS (
    SELECT 
        * 
    FROM {{ source ('apontamento', 'apontamento') }}
),

dados_tratados as (
    SELECT
        "ORDEMPRODUCAO",
        "DATAHORA",
        "QUANTIDADE", 
        "DATAHORASAIDA",   
        "DIFERENCATEMPO"
    FROM dados_brutos
), 
dados_tipados AS (
    SELECT 
        "ORDEMPRODUCAO"  ::INTEGER AS ORDER_ID,  
        "DATAHORA"      ::TIMESTAMP AS ORDER_DATE,  
        "QUANTIDADE"    ::INTEGER AS QUANTIDADE,
        "DATAHORASAIDA" ::TIMESTAMP AS TERMINO,  
        "DIFERENCATEMPO" ::DECIMAL AS TEMPO
    FROM dados_tratados
),
dados_agrupados AS (
    SELECT 
        ORDER_ID,
        ORDER_DATE,
        SUM(QUANTIDADE) AS QUANTIDADE,
        TERMINO,
        SUM(TEMPO) AS TEMPO_TOTAL_MINUTOS
    FROM dados_tipados
    GROUP BY 
        ORDER_ID,
        ORDER_DATE,
        TERMINO
)

SELECT 
 *
FROM dados_agrupados