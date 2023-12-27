WITH cartoes AS (
    SELECT
    CAST(creditcardid AS STRING) AS id_cartao
    ,CAST(cardtype AS STRING) AS tipo_cartao
    FROM {{ source('sap_adw', 'creditcard') }}
)

SELECT * FROM cartoes

-- Removi informações sensíveis como o número do cartão e data de validade. 