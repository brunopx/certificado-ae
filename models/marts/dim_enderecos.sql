WITH
    enderecos AS (
        SELECT *
        FROM {{ ref('stg_sap__enderecos') }}
    )

    ,estados AS (
        SELECT *
        FROM {{ ref('stg_sap__estados') }}
    )

    ,paises AS (
        SELECT *
        FROM {{ ref('stg_sap__paises') }}
    )

    ,int_enderecos AS (
        SELECT 
            enderecos.id_endereco
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
        FROM enderecos
        LEFT JOIN estados 
            USING (id_estado)
        LEFT JOIN paises
            USING (codigo_pais)
    )

    SELECT *
     FROM int_enderecos
