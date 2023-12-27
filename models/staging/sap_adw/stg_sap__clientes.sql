WITH clientes AS (
    SELECT
    CAST(customerid AS STRING) AS id_cliente
    ,CAST(personid AS STRING) AS id_pessoa
    ,CAST(storeid AS STRING) AS id_loja
    ,CAST(territoryid AS STRING) AS id_territorio
    ,CAST(rowguid AS STRING) AS id_row
    ,CAST (modifieddate AS DATETIME) AS cliente_dt_modificacao
    FROM {{ source('sap_adw', 'customer') }}
)

SELECT * FROM clientes
