WITH lojas AS (
    SELECT
        CAST(businessentityid AS STRING) AS id_negocio
        ,CAST(name AS STRING) AS nome_loja
        ,CAST(salespersonid AS STRING) AS nome_vendedor
        ,CAST(demographics AS STRING) AS loja_demografico
        ,CAST(rowguid AS STRING) AS loja_id_linha
        ,CAST(modifieddate AS DATETIME) AS loja_dt_modificacao
    FROM {{ source('sap_adw', 'store') }}
)

SELECT * FROM lojas
