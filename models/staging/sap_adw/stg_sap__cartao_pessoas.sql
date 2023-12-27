WITH cartao_pessoas AS (
    SELECT
    CAST(businessentityid AS STRING) AS id_negocio
    ,CAST(creditcardid AS STRING) AS id_cartao
    FROM {{ source('sap_adw', 'personcreditcard') }}
)

SELECT * FROM cartao_pessoas
