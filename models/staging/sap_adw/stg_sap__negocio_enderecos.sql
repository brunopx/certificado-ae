WITH negocio_enderecos AS (
    SELECT
        CAST(businessentityid AS STRING) AS id_negocio
        ,CAST(addressid AS STRING) AS id_endereco
        ,CAST(addresstypeid AS STRING) AS id_tipo_endereco
        ,CAST(rowguid AS STRING) AS negocio_endereco_id_linha
        ,CAST(modifieddate AS DATETIME) AS negocio_endereco_dt_modificacao
    FROM {{ source('sap_adw', 'businessentityaddress') }}
)

SELECT * FROM negocio_enderecos
