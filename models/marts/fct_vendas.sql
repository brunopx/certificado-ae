WITH 
    dim_cartoes AS (
        SELECT *
        FROM {{ ref('dim_cartoes') }}
    )
    ,dim_clientes AS (
        SELECT *
        FROM {{ ref('dim_clientes') }}
    )
    ,dim_enderecos AS (
        SELECT *
        FROM {{ ref('dim_enderecos') }}
    )
    ,dim_produtos AS (
        SELECT *
        FROM {{ ref('dim_produtos') }}
    )
    ,dim_motivos AS (
        SELECT *
        FROM {{ ref('dim_motivos') }}
    )
    ,int_vendas_detalhe AS (
        SELECT *
        FROM {{ ref('int_vendas__vendas_detalhe') }}
    )
    ,join_tabelas AS (
        SELECT 
            (int_vendas_detalhe.sk_venda_produto||CASE WHEN dim_motivos.id_motivo IS NULL THEN "" ELSE dim_motivos.id_motivo END) AS pk_venda_produto_motivo
            ,int_vendas_detalhe.sk_venda_produto
            ,int_vendas_detalhe.id_venda_detalhe
            ,int_vendas_detalhe.id_venda
            ,int_vendas_detalhe.id_cliente
            ,int_vendas_detalhe.id_produto
            ,dim_motivos.id_motivo
            ,dim_clientes.pessoa_nome_completo
            -- ,int_vendas_detalhe.id_cartao
            -- ,int_vendas_detalhe.id_endereco_fatura
            -- ,int_vendas_detalhe.id_endereco_entrega
            ,int_vendas_detalhe.qtde_produto
            ,int_vendas_detalhe.preco_unitario
            ,int_vendas_detalhe.desconto_preco_unitario
            ,int_vendas_detalhe.data_venda
            -- ,int_vendas_detalhe.prazo_venda
            -- ,int_vendas_detalhe.dt_envio_venda
            ,int_vendas_detalhe.status_venda
            -- ,int_vendas_detalhe.is_online
            ,int_vendas_detalhe.subtotal
            ,int_vendas_detalhe.taxa_venda
            ,int_vendas_detalhe.frete_venda
            ,int_vendas_detalhe.valor_total
            -- ,dim_cartoes.id_negocio
            ,dim_cartoes.tipo_cartao
            -- ,dim_enderecos.id_endereco
            -- ,dim_enderecos.endereco_completo
            ,dim_enderecos.endereco_cidade
            -- ,dim_enderecos.cep
            -- ,dim_enderecos.codigo_estado
            -- ,dim_enderecos.codigo_pais
            ,dim_enderecos.nome_estado
            ,dim_enderecos.nome_pais
            ,dim_produtos.nome_produto
            -- ,dim_produtos.numero_produto
            -- ,dim_produtos.cor_produto
            -- ,dim_produtos.custo_padrao
            -- ,dim_produtos.preco_lista
            -- ,dim_produtos.tamanho_produto
            -- ,dim_produtos.unidade_medida_tamanho
            -- ,dim_produtos.unidade_medida_peso
            -- ,dim_produtos.peso_produto
            ,dim_motivos.motivo
            ,dim_motivos.tipo_motivo
        FROM int_vendas_detalhe
        LEFT JOIN dim_cartoes
            USING (id_cartao)
        LEFT JOIN dim_clientes
            ON (int_vendas_detalhe.id_cliente = dim_clientes.pk_cliente)
        LEFT JOIN dim_enderecos
            ON (int_vendas_detalhe.id_endereco_entrega = dim_enderecos.id_endereco)
        LEFT JOIN dim_produtos
            USING (id_produto)
        LEFT JOIN dim_motivos
            USING (id_venda)
    )
    ,contagem_duplicacoes AS (
        SELECT
            *
            ,COUNT (*) OVER (PARTITION BY sk_venda_produto) AS qtde_motivos_por_venda_produto
            ,COUNT (DISTINCT id_produto) OVER (PARTITION BY id_venda) AS qtde_produto_por_venda
        FROM join_tabelas
    )
    ,deduplicacao_valores AS (
        SELECT
            * EXCEPT (qtde_motivos_por_venda_produto,qtde_produto_por_venda)
            ,qtde_produto/qtde_motivos_por_venda_produto AS qtde_produto_parc
            ,preco_unitario/qtde_motivos_por_venda_produto AS preco_unitario_parc
            ,desconto_preco_unitario/qtde_motivos_por_venda_produto AS desconto_preco_unitario_parc
            ,subtotal/(qtde_motivos_por_venda_produto*qtde_produto_por_venda) AS subtotal_parc
            ,taxa_venda/(qtde_motivos_por_venda_produto*qtde_produto_por_venda) AS taxa_venda_parc
            ,frete_venda/(qtde_motivos_por_venda_produto*qtde_produto_por_venda) AS frete_venda_parc
            ,valor_total/(qtde_motivos_por_venda_produto*qtde_produto_por_venda) AS valor_total_parc
            FROM contagem_duplicacoes
    )
    ,totais_venda_produto AS (
        SELECT 
            *
            ,SUM(qtde_produto_parc) OVER (PARTITION BY sk_venda_produto) * SUM(preco_unitario_parc) OVER (PARTITION BY sk_venda_produto) AS valor_total_negociado_venda_produto
            ,(SUM(qtde_produto_parc) OVER (PARTITION BY sk_venda_produto) * SUM(preco_unitario_parc) OVER (PARTITION BY sk_venda_produto)) - (1- SUM (desconto_preco_unitario_parc) OVER (PARTITION BY sk_venda_produto)) AS valor_total_negociado_liquido_venda_produto
            ,ROW_NUMBER() OVER (PARTITION BY sk_venda_produto ORDER BY id_motivo) AS motivo_index
        FROM deduplicacao_valores
    )
    



    SELECT * FROM totais_venda_produto
