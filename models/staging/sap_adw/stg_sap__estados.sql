WITH estados AS (
    SELECT
        CAST(stateprovinceid AS STRING) AS id_estado
        ,CAST(stateprovincecode AS STRING) AS codigo_estado
        ,CAST(countryregioncode AS STRING) AS codigo_pais
        ,CAST(isonlystateprovinceflag AS BOOLEAN) AS is_estado_provincia
        ,CAST(name AS STRING) AS nome_estado
        ,CAST(territoryid AS STRING) AS id_territorio
        ,CAST(rowguid AS STRING) AS estado_id_linha
        ,CAST(modifieddate AS DATETIME) AS estado_dt_modificacao
    FROM {{ source('sap_adw', 'stateprovince') }}
)

SELECT * FROM estados
