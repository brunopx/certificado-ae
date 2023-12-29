WITH
    vendas AS (
        SELECT *
        FROM {{ ref('stg_sap__vendas') }}
    )
    ,vendas_detalhe AS (
        SELECT *
        FROM {{ ref('stg_sap__vendas_detalhe') }}
    )
    ,join_tabelas AS (
        SELECT 
            vendas_detalhe.id_venda_detalhe
            ,vendas_detalhe.id_venda
            ,vendas_detalhe.id_produto
            -- ,vendas_detalhe.id_oferta_especial
            -- ,vendas_detalhe.rastreamento_venda
            ,vendas_detalhe.qtde_produto
            ,vendas_detalhe.preco_unitario
            ,vendas_detalhe.desconto_preco_unitario
            -- ,vendas_detalhe.venda_detalhe_id_linha
            -- ,vendas_detalhe.venda_detalhe_dt_modificacao
            -- ,vendas.numero_revisao
            ,vendas.data_venda
            ,vendas.prazo_venda
            ,vendas.dt_envio_venda
            ,vendas.status_venda
            ,vendas.is_online
            -- ,vendas.numero_venda
            -- ,vendas.numero_conta_venda
            ,vendas.id_cliente
            -- ,vendas.id_vendedor
            -- ,vendas.id_territorio
            ,vendas.id_endereco_fatura
            ,vendas.id_endereco_entrega
            -- ,vendas.id_metodo_entrega
            ,vendas.id_cartao
            -- ,vendas.cod_apr_cartao
            -- ,vendas.id_moeda
            ,vendas.subtotal
            ,vendas.taxa_venda
            ,vendas.frete_venda
            ,vendas.valor_total
            -- ,vendas.comentario
            -- ,vendas.venda_id_linha
            -- ,vendas.venda_dt_modificacao
        FROM vendas_detalhe
        LEFT JOIN vendas
            USING (id_venda)

    )

    ,criar_chave AS(
        SELECT
            id_venda||id_produto AS sk_venda_produto
            ,*
        FROM join_tabelas       
    )

    SELECT * FROM criar_chave
