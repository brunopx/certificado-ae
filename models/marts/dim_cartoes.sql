WITH
    cartoes AS (
        SELECT *
        FROM {{ ref('stg_sap__cartoes') }}
    )

    ,cartao_pessoas AS (
        SELECT *
        FROM {{ ref('stg_sap__cartao_pessoas') }} -- Poderia ter feito somente com a stg_sap__cartoes, mas tinha construído algo mais complexo e não quis apagar tudo que eu fiz antes.
    )

    ,int_cartoes AS (
        SELECT 
            cartao_pessoas.id_cartao
            ,cartao_pessoas.id_negocio
            ,cartoes.tipo_cartao
        FROM cartao_pessoas
        LEFT JOIN cartoes 
            USING (id_cartao)
    )

    SELECT *
     FROM int_cartoes
