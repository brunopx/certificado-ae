WITH paises AS (
    SELECT
        CAST(countryregioncode AS STRING) AS codigo_pais
        ,CAST(name AS STRING) AS nome_pais
        ,CAST(modifieddate AS DATETIME) AS pais_dt_modificacao 
    FROM {{ source('sap_adw', 'countryregion') }}
)

SELECT * FROM paises
