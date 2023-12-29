WITH 
    base AS (
        SELECT * 
        FROM {{ ref('fct_vendas') }}
    )
    ,calculo AS (
        SELECT
            CAST(DATE_TRUNC(data_venda, YEAR) AS DATE) AS ano
            ,ROUND(SUM(valor_total_negociado_venda_produto),2) AS venda_bruta
        FROM base 
        WHERE motivo_index = 1
        GROUP BY 1
    )
    SELECT 
        * 
    FROM calculo 
    WHERE 
        ano = "2011-01-01" 
        AND venda_bruta  <> 12646112.16
