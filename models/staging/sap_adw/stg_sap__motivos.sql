WITH motivos AS (
    SELECT 
        CAST(salesreasonid AS STRING) AS id_motivo
        ,CAST(name AS STRING) AS motivo
        ,CAST(reasontype AS STRING) AS tipo_motivo
        ,CAST(modifieddate AS STRING) AS motivo_dt_modificacao
    FROM {{ source('sap_adw', 'salesreason') }}
)

SELECT * FROM motivos

