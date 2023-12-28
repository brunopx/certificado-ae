WITH vendas_detalhes AS (
    SELECT 
        CAST(salesorderdetailid AS STRING) AS id_venda_detalhe
        ,CAST(salesorderid AS STRING) AS id_venda
        ,CAST(productid AS STRING) AS id_produto
        ,CAST(specialofferid AS STRING) AS id_oferta_especial
        ,CAST(carriertrackingnumber AS STRING) AS rastreamento_venda
        ,CAST(orderqty AS INT) AS qtde_produto
        ,CAST(unitprice AS DECIMAL) AS preco_unitario 
        ,CAST(unitpricediscount AS DECIMAL) AS desconto_preco_unitario
        ,CAST(rowguid AS STRING) AS venda_detalhe_id_linha
        ,CAST(modifieddate AS DATETIME) AS venda_detalhe_dt_modificacao
    FROM {{ source('sap_adw', 'salesorderdetail') }}
)

SELECT * FROM vendas_detalhes

