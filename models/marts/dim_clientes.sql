WITH
    clientes AS (
        SELECT *
        FROM {{ ref('stg_sap__clientes') }}
    )

    ,pessoas AS (
        SELECT *
        FROM {{ ref('stg_sap__pessoas') }}
    )

    ,final_join AS (
        SELECT
        clientes.id_cliente AS pk_cliente
        ,clientes.id_pessoa
        ,clientes.id_loja
        ,pessoas.pessoa_nome_completo
        ,clientes.id_territorio
        FROM
        clientes
        LEFT JOIN pessoas
            ON (clientes.id_cliente = pessoas.id_negocio)
    )

    SELECT *
     FROM final_join
