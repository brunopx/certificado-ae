WITH vendas AS (
    SELECT 
        CAST(salesorderid AS STRING) AS id_venda
        ,CAST(revisionnumber AS STRING) AS numero_revisao
        ,CAST(orderdate AS DATETIME) AS data_venda
        ,CAST(duedate AS DATETIME) AS prazo_venda
        ,CAST(shipdate AS DATETIME) AS dt_envio_venda
        ,CAST(status AS STRING) AS status_venda
        ,CAST(onlineorderflag AS BOOLEAN) AS is_online 
        ,CAST(purchaseordernumber AS STRING) AS numero_venda
        ,CAST(accountnumber AS STRING) AS numero_conta_venda
        ,CAST(customerid AS STRING) AS id_cliente
        ,CAST(salespersonid AS STRING) AS id_vendedor
        ,CAST(territoryid AS STRING) AS id_territorio
        ,CAST(billtoaddressid AS STRING) AS id_endereco_fatura
        ,CAST(shiptoaddressid AS STRING) AS id_endereco_entrega
        ,CAST(shipmethodid AS STRING) AS id_metodo_entrega
        ,CAST(creditcardid AS STRING) AS id_cartao
        ,CAST(creditcardapprovalcode AS STRING) AS cod_apr_cartao 
        ,CAST(currencyrateid AS STRING) AS id_moeda
        ,CAST(subtotal AS DECIMAL) AS subtotal
        ,CAST(taxamt AS DECIMAL) AS taxa_venda
        ,CAST(freight AS DECIMAL) AS frete_venda
        ,CAST(totaldue AS DECIMAL) AS valor_total
        ,CAST(comment AS STRING) AS comentario
        ,CAST(rowguid AS STRING) AS venda_id_linha
        ,CAST(modifieddate AS DATETIME) AS venda_dt_modificacao
    FROM {{ source('sap_adw', 'salesorderheader') }}
)

SELECT * FROM vendas

