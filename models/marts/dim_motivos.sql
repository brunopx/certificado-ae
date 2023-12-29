WITH
    vendas_motivos AS (
        SELECT *
        FROM {{ ref('stg_sap__vendas_motivos') }}
    )

    ,motivos AS (
        SELECT *
        FROM {{ ref('stg_sap__motivos') }} 
    )    

    ,join_tables AS (
        SELECT 
            vendas_motivos.id_venda||motivos.id_motivo AS pk_venda_motivo
            ,vendas_motivos.id_venda
            ,motivos.id_motivo
            ,motivos.motivo
            ,motivos.tipo_motivo
        FROM vendas_motivos 
        LEFT JOIN motivos
            USING (id_motivo)
    )

    SELECT * from join_tables