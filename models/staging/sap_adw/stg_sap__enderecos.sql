WITH enderecos AS (
    SELECT
        CAST(addressid AS STRING) AS id_endereco
        ,CAST(addressline1 AS STRING) AS endereco_linha1
        ,CAST(addressline2 AS STRING) AS endereco_linha2
        ,CAST(city AS STRING) AS endereco_cidade
        ,CAST(stateprovinceid AS STRING) AS id_estado
        ,CAST(postalcode AS STRING) AS cep
        ,CAST(spatiallocation AS STRING) AS alocacao 
        ,CAST(rowguid AS STRING) AS endereco_id_linha
        ,CAST(modifieddate AS DATETIME) AS endereco_dt_modificacao
    FROM {{ source('sap_adw', 'address') }}
)

SELECT * FROM enderecos
