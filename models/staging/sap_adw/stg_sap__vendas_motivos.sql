WITH vendas_motivos AS (
    SELECT 
        CAST(salesorderid AS STRING) AS id_venda
        ,CAST(salesreasonid AS STRING) AS id_motivo
        ,CAST(modifieddate AS DATETIME) AS venda_motivo_dt_modificacao
    FROM {{ source('sap_adw', 'salesorderheadersalesreason') }}
)

SELECT * FROM vendas_motivos

