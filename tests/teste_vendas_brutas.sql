WITH 
    base AS (
        SELECT * 
        FROM {{ ref('fct_vendas') }}
    )
    ,calculo AS (
        SELECT
            CAST(DATE_TRUNC(data_venda, YEAR) AS DATE) AS ano
            ,sk_venda_produto
            ,SUM(qtde_produto_parc) AS qtde_produto
            ,SUM (preco_unitario_parc) AS preco_unitario
        FROM base 
        GROUP BY 1,2
    )
    ,teste AS(
        SELECT
            ano
            ,ROUND(SUM(qtde_produto*preco_unitario),2) AS venda_bruta
        FROM calculo
        GROUP BY 1
    )

        SELECT 
        * 
        FROM teste 
        WHERE 
            ano = "2011-01-01" 
            AND venda_bruta  <> 12646112.16
