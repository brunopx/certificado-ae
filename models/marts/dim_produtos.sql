WITH
    produtos AS (
        SELECT *
        FROM {{ ref('stg_sap__produtos') }}
    )

    ,produtos_col_interesse AS (
        SELECT
        id_produto
        ,nome_produto
        ,numero_produto
        -- ,is_make
        -- ,is_finished_goods
        ,cor_produto
        -- ,nivel_estoque_seguro
        -- ,ponto_reordem_produto
        ,custo_padrao
        ,preco_lista
        ,tamanho_produto
        ,unidade_medida_tamanho
        ,unidade_medida_peso
        ,peso_produto
        -- ,dias_fabricacao
        -- ,linha_produto
        -- ,classe_produto
        -- ,estilo_produto
        -- ,subcategoria_produto
        -- ,id_modelo_produto
        -- ,dt_inicio_venda_produto
        -- ,dt_fim_venda_produto
        -- ,dt_descont_produto
        -- ,produto_id_linha
        -- ,produto_dt_modificacao
        FROM
        produtos

    )

    SELECT *
     FROM produtos_col_interesse
