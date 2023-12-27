WITH
    cartoes AS (
        SELECT *
        FROM {{ ref('stg_sap__cartoes') }}
    )

    ,cartao_pessoas AS (
        SELECT *
        FROM {{ ref('stg_sap__cartao_pessoas') }}
    )

    ,clientes AS (
        SELECT *
        FROM {{ ref('stg_sap__clientes') }}
    )

    ,pessoas AS (
        SELECT *
        FROM {{ ref('stg_sap__pessoas') }}
    )

    ,enderecos AS (
        SELECT *
        FROM {{ ref('stg_sap__enderecos') }}
    )

    ,negocio_enderecos AS (
        SELECT *
        FROM {{ ref('stg_sap__negocio_enderecos') }}
    )

    ,estados AS (
        SELECT *
        FROM {{ ref('stg_sap__estados') }}
    )

    ,paises AS (
        SELECT *
        FROM {{ ref('stg_sap__paises') }}
    )

    ,int_cartoes AS (
        SELECT 
            cartao_pessoas.id_cartao
            ,cartao_pessoas.id_negocio
            ,cartoes.tipo_cartao
        FROM cartao_pessoas
        LEFT JOIN cartoes 
            USING (id_cartao)
    )

    ,int_enderecos AS (
        SELECT 
            negocio_enderecos.id_negocio
            ,enderecos.id_endereco
            --,enderecos.endereco_linha1
            --,enderecos.endereco_linha2
            ,enderecos.endereco_linha1||COALESCE(enderecos.endereco_linha2,"") AS endereco_completo
            ,enderecos.endereco_cidade
            --,enderecos.id_estado
            ,enderecos.cep
            --,enderecos.alocacao
            --,enderecos.endereco_id_linha
            --,enderecos.endereco_dt_modificacao
            --,estados.id_estado
            ,estados.codigo_estado
            ,estados.codigo_pais
            --,estados.is_estado_provincia
            ,estados.nome_estado
            --,estados.id_territorio
            --,estados.estado_id_linha
            --,estados.estado_dt_modificacao
            --,paises.codigo_pais
            ,paises.nome_pais
            --,paises.pais_dt_modificacao
        FROM negocio_enderecos
        LEFT JOIN enderecos
            USING (id_endereco)
        LEFT JOIN estados 
            USING (id_estado)
        LEFT JOIN paises
            USING (codigo_pais)
    )
    ,final_join AS (
        SELECT
        clientes.id_cliente AS pk_cliente
        ,clientes.id_pessoa
        ,clientes.id_loja
        ,pessoas.pessoa_nome_completo
        ,clientes.id_territorio
        --,int_enderecos.id_negocio
        ,int_enderecos.id_endereco
        ,int_enderecos.endereco_completo
        ,int_enderecos.endereco_cidade
        ,int_enderecos.cep
        ,int_enderecos.codigo_estado
        ,int_enderecos.codigo_pais
        ,int_enderecos.nome_estado
        ,int_enderecos.nome_pais
        ,int_cartoes.id_cartao
        ,int_cartoes.tipo_cartao
        FROM
        clientes
        LEFT JOIN pessoas
            ON (clientes.id_cliente = pessoas.id_negocio)
        LEFT JOIN int_enderecos
            ON (clientes.id_cliente = int_enderecos.id_negocio)
        LEFT JOIN int_cartoes
            ON (clientes.id_cliente = int_cartoes.id_negocio)
    )

    SELECT *
     FROM final_join
