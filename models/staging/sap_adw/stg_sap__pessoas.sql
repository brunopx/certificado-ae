WITH pessoas AS (
    SELECT
        CAST(businessentityid AS STRING) AS id_negocio
        ,CAST(persontype AS STRING) AS tipo_pessoa
        ,CAST(namestyle AS STRING) AS nome_estilo
        ,CAST(title AS STRING) AS titulo_pessoa
        ,CAST(firstname AS STRING) AS nome_pessoa
        ,CAST(middlename AS STRING) AS nome_meio_pessoa
        ,CAST(lastname AS STRING) AS sobrenome_pessoa
        ,CAST(suffix AS STRING) AS sufixo_pessoa
        ,CAST(emailpromotion AS INT) AS promocao_email
        ,CAST(additionalcontactinfo AS STRING) AS contato_adicional_pessoa
        ,CAST(demographics AS STRING) AS pessoa_demografico
        ,CAST(rowguid AS STRING) AS pessoa_id_row
        ,CAST(modifieddate AS DATETIME) AS pessoa_dt_modificacao
    FROM {{ source('sap_adw', 'person') }}
)

SELECT *, CONCAT (nome_pessoa, " ", sobrenome_pessoa) AS pessoa_nome_completo FROM pessoas
