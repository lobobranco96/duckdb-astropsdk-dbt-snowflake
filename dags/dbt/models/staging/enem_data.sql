{{ config(
    file_format="delta",
    materialized="view"
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY NU_INSCRICAO) AS ID,
    NU_INSCRICAO AS NU_INSCRICAO,
    NU_ANO AS NU_ANO,
    CAST(TP_FAIXA_ETARIA AS VARCHAR(50)) AS TP_FAIXA_ETARIA,
    CAST(TP_SEXO AS VARCHAR(50)) AS TP_SEXO,
    CAST(TP_ESTADO_CIVIL AS VARCHAR(50)) AS TP_ESTADO_CIVIL,
    CAST(TP_COR_RACA AS VARCHAR(50)) AS TP_COR_RACA,
    CAST(TP_NACIONALIDADE AS VARCHAR(50)) AS TP_NACIONALIDADE,
    CAST(TP_ST_CONCLUSAO AS VARCHAR(50)) AS TP_ST_CONCLUSAO,
    CAST(TP_ANO_CONCLUIU AS VARCHAR(50)) AS TP_ANO_CONCLUIU,
    CAST(TP_ESCOLA AS VARCHAR(50)) AS TP_ESCOLA,
    CAST(TP_ENSINO AS VARCHAR(50)) AS TP_ENSINO,
    CAST(IN_TREINEIRO AS VARCHAR(50)) AS IN_TREINEIRO,
    CO_MUNICIPIO_ESC AS CO_MUNICIPIO_ESC,
    NO_MUNICIPIO_ESC AS NO_MUNICIPIO_ESC,
    CO_UF_ESC AS CO_UF_ESC,
    SG_UF_ESC AS SG_UF_ESC,
    CAST(TP_DEPENDENCIA_ADM_ESC AS VARCHAR(50)) AS TP_DEPENDENCIA_ADM_ESC,
    CAST(TP_LOCALIZACAO_ESC AS VARCHAR(50)) AS TP_LOCALIZACAO_ESC,
    CAST(TP_SIT_FUNC_ESC AS VARCHAR(50)) AS TP_SIT_FUNC_ESC,
    CO_MUNICIPIO_PROVA AS CO_MUNICIPIO_PROVA,
    NO_MUNICIPIO_PROVA AS NO_MUNICIPIO_PROVA,
    CO_UF_PROVA AS CO_UF_PROVA,
    SG_UF_PROVA AS SG_UF_PROVA,
    CAST(TP_PRESENCA_CN AS VARCHAR(50)) AS TP_PRESENCA_CN,
    CAST(TP_PRESENCA_CH AS VARCHAR(50)) AS TP_PRESENCA_CH,
    CAST(TP_PRESENCA_LC AS VARCHAR(50)) AS TP_PRESENCA_LC,
    CAST(TP_PRESENCA_MT AS VARCHAR(50)) AS TP_PRESENCA_MT,
    CAST(CO_PROVA_CN AS VARCHAR(50)) AS CO_PROVA_CN,
    CAST(CO_PROVA_CH AS VARCHAR(50)) AS CO_PROVA_CH,
    CAST(CO_PROVA_LC AS VARCHAR(50)) AS CO_PROVA_LC,
    CAST(CO_PROVA_MT AS VARCHAR(50)) AS CO_PROVA_MT,
    NU_NOTA_CH AS NU_NOTA_CH,
    NU_NOTA_CN AS NU_NOTA_CN,
    NU_NOTA_LC AS NU_NOTA_LC,
    NU_NOTA_MT AS NU_NOTA_MT,
    TX_RESPOSTAS_CH AS TX_RESPOSTAS_CH,
    TX_RESPOSTAS_CN AS TX_RESPOSTAS_CN,
    TX_RESPOSTAS_LC AS TX_RESPOSTAS_LC,
    TX_RESPOSTAS_MT AS TX_RESPOSTAS_MT,
    CAST(TP_LINGUA AS VARCHAR(50)) AS TP_LINGUA,
    TX_GABARITO_CH AS TX_GABARITO_CH,
    TX_GABARITO_CN AS TX_GABARITO_CN,
    TX_GABARITO_LC AS TX_GABARITO_LC,
    TX_GABARITO_MT AS TX_GABARITO_MT,
    CAST(TP_STATUS_REDACAO AS VARCHAR(50)) AS TP_STATUS_REDACAO,
    NU_NOTA_COMP1 AS NU_NOTA_COMP1,
    NU_NOTA_COMP2 AS NU_NOTA_COMP2,
    NU_NOTA_COMP3 AS NU_NOTA_COMP3,
    NU_NOTA_COMP4 AS NU_NOTA_COMP4,
    NU_NOTA_COMP5 AS NU_NOTA_COMP5,
    NU_NOTA_REDACAO AS NU_NOTA_REDACAO,
    Q001 AS Q001,
    Q002 AS Q002,
    Q003 AS Q003,
    Q004 AS Q004,
    Q005 AS Q005,
    Q006 AS Q006,
    Q007 AS Q007,
    Q008 AS Q008,
    Q009 AS Q009,
    Q010 AS Q010,
    Q011 AS Q011,
    Q012 AS Q012,
    Q013 AS Q013,
    Q014 AS Q014,
    Q015 AS Q015,
    Q016 AS Q016,
    Q017 AS Q017,
    Q018 AS Q018,
    Q019 AS Q019,
    Q020 AS Q020,
    Q021 AS Q021,
    Q022 AS Q022,
    Q023 AS Q023,
    Q024 AS Q024,
    Q025 AS Q025
FROM {{ source('snowflake', 'MERGED_ENEMDATA') }}

