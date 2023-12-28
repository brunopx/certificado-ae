WITH produtos AS (
    SELECT 
        CAST(productid AS STRING) AS id_produto
        ,CAST(name AS STRING) AS nome_produto
        ,CAST(productnumber AS STRING) AS numero_produto
        ,CAST(makeflag AS BOOLEAN) AS is_make
        ,CAST(finishedgoodsflag AS STRING) AS is_finished_goods
        ,CAST(color AS STRING) AS cor_produto
        ,CAST(safetystocklevel AS INT) AS nivel_estoque_seguro
        ,CAST(reorderpoint AS STRING) AS ponto_reordem_produto
        ,CAST(standardcost AS DECIMAL) AS custo_padrao
        ,CAST(listprice AS DECIMAL) AS preco_lista
        ,CAST(size AS STRING) AS tamanho_produto
        ,CAST(sizeunitmeasurecode AS STRING) AS unidade_medida_tamanho
        ,CAST(weightunitmeasurecode AS STRING) AS unidade_medida_peso
        ,CAST(weight AS DECIMAL) AS peso_produto
        ,CAST(daystomanufacture AS INT) AS dias_fabricacao
        ,CAST(productline AS STRING) AS linha_produto
        ,CAST(class AS STRING) AS classe_produto
        ,CAST(style AS STRING) AS estilo_produto
        ,CAST(productsubcategoryid AS STRING) AS subcategoria_produto
        ,CAST(productmodelid AS STRING) AS id_modelo_produto
        ,CAST(sellstartdate AS DATETIME) AS dt_inicio_venda_produto
        ,CAST(sellenddate AS DATETIME) AS dt_fim_venda_produto
        ,CAST(discontinueddate AS STRING) AS dt_descont_produto
        ,CAST(rowguid AS STRING) AS produto_id_linha
        ,CAST(modifieddate AS STRING) AS produto_dt_modificacao
    FROM {{ source('sap_adw', 'product') }}
)

SELECT * FROM produtos

