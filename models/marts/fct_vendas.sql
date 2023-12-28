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
    ,int_vendas_detalhe AS (
        SELECT *
        FROM {{ ref('int_vendas__vendas_detalhe') }}
    )
    ,join_tabelas AS (
        SELECT 
            int_vendas_detalhe.sk_venda_detalhe
            ,int_vendas_detalhe.id_venda_detalhe
            ,int_vendas_detalhe.id_venda
            ,int_vendas_detalhe.id_cliente
            ,int_vendas_detalhe.id_produto
            ,int_vendas_detalhe.id_cartao
            ,int_vendas_detalhe.id_endereco_fatura
            ,int_vendas_detalhe.id_endereco_entrega
            ,int_vendas_detalhe.qtde_produto
            ,int_vendas_detalhe.preco_unitario
            ,int_vendas_detalhe.desconto_preco_unitario
            ,int_vendas_detalhe.data_venda
            ,int_vendas_detalhe.prazo_venda
            ,int_vendas_detalhe.dt_envio_venda
            ,int_vendas_detalhe.status_venda
            ,int_vendas_detalhe.is_online
            ,int_vendas_detalhe.subtotal
            ,int_vendas_detalhe.taxa_venda
            ,int_vendas_detalhe.frete_venda
            ,int_vendas_detalhe.valor_total
            -- ,dim_cartoes.id_negocio
            ,dim_cartoes.tipo_cartao
            -- ,dim_enderecos.id_endereco
            ,dim_enderecos.endereco_completo
            ,dim_enderecos.endereco_cidade
            ,dim_enderecos.cep
            ,dim_enderecos.codigo_estado
            ,dim_enderecos.codigo_pais
            ,dim_enderecos.nome_estado
            ,dim_enderecos.nome_pais
            ,dim_produtos.nome_produto
            ,dim_produtos.numero_produto
            ,dim_produtos.cor_produto
            ,dim_produtos.custo_padrao
            ,dim_produtos.preco_lista
            ,dim_produtos.tamanho_produto
            ,dim_produtos.unidade_medida_tamanho
            ,dim_produtos.unidade_medida_peso
            ,dim_produtos.peso_produto
        FROM int_vendas_detalhe
        LEFT JOIN dim_cartoes
            USING (id_cartao)
        LEFT JOIN dim_clientes
            ON (int_vendas_detalhe.id_cliente = dim_clientes.pk_cliente)
        LEFT JOIN dim_enderecos
            ON (int_vendas_detalhe.id_endereco_entrega = dim_enderecos.id_endereco)
        LEFT JOIN dim_produtos
            USING (id_produto)
    )
    SELECT * FROM join_tabelas