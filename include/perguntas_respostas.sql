-- 1° primeira pergunta: Qual a escola com a maior média de notas por ano?

WITH MediaPorEscola AS (
    SELECT 
        dp.NU_ANO,
        de.CO_MUNICIPIO_ESC, 
        de.NO_MUNICIPIO_ESC,
        de.CO_UF_ESC,
        de.SG_UF_ESC,
        de.TP_DEPENDENCIA_ADM_ESC AS Dependencia_Administrativa,
        de.TP_LOCALIZACAO_ESC AS Localizacao_Escola,
        AVG(
            (CAST(dpo.NU_NOTA_CN AS DECIMAL(10, 2)) + 
             CAST(dpo.NU_NOTA_CH AS DECIMAL(10, 2)) + 
             CAST(dpo.NU_NOTA_LC AS DECIMAL(10, 2)) + 
             CAST(dpo.NU_NOTA_MT AS DECIMAL(10, 2)) + 
             CAST(dr.NU_NOTA_REDACAO AS DECIMAL(10, 2)))
            / 5.0
        ) AS MEDIA_TOTAL
    FROM ENEM_STORAGE.DATA.FATO_ENEM fe
    JOIN ENEM_STORAGE.DATA.dim_escola de ON de.ID_ESCOLA = fe.ID_ESCOLA
    JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
    JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
    JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
    GROUP BY
        dp.NU_ANO,
        de.CO_MUNICIPIO_ESC, 
        de.NO_MUNICIPIO_ESC,
        de.CO_UF_ESC,
        de.SG_UF_ESC,
        de.TP_DEPENDENCIA_ADM_ESC,
        de.TP_LOCALIZACAO_ESC
)

SELECT *
FROM MediaPorEscola
WHERE (NU_ANO, MEDIA_TOTAL) IN (
    SELECT NU_ANO, MAX(MEDIA_TOTAL)
    FROM MediaPorEscola
    GROUP BY NU_ANO
)
ORDER BY NU_ANO, MEDIA_TOTAL DESC;



-- 2° pergunta: Os 5 alunos com as maiores média de notas e o valor dessa média por cada ano de realizaçao do exame do enem?

WITH MediaPorAno AS (
    SELECT 
        dp.NU_ANO,
        dp.NU_INSCRICAO, 
        (AVG(dpo.NU_NOTA_CN) + AVG(dpo.NU_NOTA_CH) + AVG(dpo.NU_NOTA_LC) + AVG(dpo.NU_NOTA_MT) + AVG(dr.NU_NOTA_REDACAO)) / 5 AS MEDIA_TOTAL,
        ROW_NUMBER() OVER (PARTITION BY dp.NU_ANO ORDER BY (AVG(dpo.NU_NOTA_CN) + AVG(dpo.NU_NOTA_CH) + AVG(dpo.NU_NOTA_LC) + AVG(dpo.NU_NOTA_MT) + AVG(dr.NU_NOTA_REDACAO)) / 5 DESC) AS rn
    FROM ENEM_STORAGE.DATA.FATO_ENEM fe
    INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
    INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
    INNER JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
    GROUP BY 
        dp.NU_ANO,
        dp.NU_INSCRICAO
    HAVING MEDIA_TOTAL IS NOT NULL
)

SELECT NU_ANO, NU_INSCRICAO, MEDIA_TOTAL
FROM MediaPorAno
WHERE rn <= 5
ORDER BY NU_ANO, MEDIA_TOTAL DESC;

-- 3 - Qual a média geral por ano?

SELECT
    dp.NU_ANO,
    TO_CHAR(ROUND(AVG(
        (dpo.NU_NOTA_CN + dpo.NU_NOTA_CH + dpo.NU_NOTA_LC + dpo.NU_NOTA_MT + dr.NU_NOTA_REDACAO) / 5
    ), 2), 'FM999999999.00') AS MEDIA_GERAL
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
INNER JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.NU_ANO
ORDER BY MEDIA_GERAL DESC;


-- 4 - Qual o % de Ausentes por ano?

SELECT 
    dp.NU_ANO,
    TO_CHAR(SUM(CASE WHEN dpo.TP_PRESENCA_CN = 'Presente na prova' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 'FM999999999.00') AS "Ciencias da Natureza % Ausentes",
    TO_CHAR(SUM(CASE WHEN dpo.TP_PRESENCA_CH = 'Presente na prova' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 'FM999999999.00') AS "Ciencias Humanas % Ausentes",
    TO_CHAR(SUM(CASE WHEN dpo.TP_PRESENCA_LC = 'Presente na prova' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 'FM999999999.00') AS "Linguagens e Códigos % Ausentes",
    TO_CHAR(SUM(CASE WHEN dpo.TP_PRESENCA_MT = 'Presente na prova' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 'FM999999999.00') AS "Matemática % Ausentes"
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.NU_ANO
ORDER BY dp.NU_ANO DESC;

-- 5 - Qual o número total de Inscritos por ano?

SELECT
    dp.NU_ANO,
    COUNT(dp.ID_PARTICIPANTE) AS "Total Inscritos"
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.NU_ANO
ORDER BY dp.NU_ANO;

-- 6 Qual a média por disciplina por ano?

SELECT 
    dp.NU_ANO,
    TO_CHAR(AVG(dpo.NU_NOTA_CN), 'FM999999999.00') AS "Media Ciências da Natureza",
    TO_CHAR(AVG(dpo.NU_NOTA_CH), 'FM999999999.00') AS "Media Ciências Humanas",
    TO_CHAR(AVG(dpo.NU_NOTA_LC), 'FM999999999.00') AS "Media Linguagens e Códigos",
    TO_CHAR(AVG(dpo.NU_NOTA_MT), 'FM999999999.00') AS "Media Matemática",
    TO_CHAR(AVG(dr.NU_NOTA_REDACAO), 'FM999999999.00') AS "Media Redação"
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
INNER JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.NU_ANO
ORDER BY dp.NU_ANO;

-- 7 - Qual a média por Sexo em cada ano?

SELECT 
    dp.NU_ANO,
    dp.TP_SEXO AS "Sexo do Participante",
    COUNT(*) AS "Quantidade",
    TO_CHAR(AVG(dpo.NU_NOTA_CN), 'FM999999999.00') AS "Media Ciências da Natureza",
    TO_CHAR(AVG(dpo.NU_NOTA_CH), 'FM999999999.00') AS "Media Ciências Humanas",
    TO_CHAR(AVG(dpo.NU_NOTA_LC), 'FM999999999.00') AS "Media Linguagens e Códigos",
    TO_CHAR(AVG(dpo.NU_NOTA_MT), 'FM999999999.00') AS "Media Matemática",
    TO_CHAR(AVG(dr.NU_NOTA_REDACAO), 'FM999999999.00') AS "Media Redação"
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
INNER JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.TP_SEXO, dp.NU_ANO
ORDER BY dp.NU_ANO DESC;

-- 8 - Qual a média por Etnia?


SELECT 
    dp.NU_ANO,
    CASE 
        WHEN dp.TP_COR_RACA = '6' THEN 'Outra'
    ELSE dp.TP_COR_RACA 
    END AS "Etnia do Participante",
    COUNT(*) AS "Quantidade",
    TO_CHAR(AVG(dpo.NU_NOTA_CN), 'FM999999999.00') AS "Media Ciências da Natureza",
    TO_CHAR(AVG(dpo.NU_NOTA_CH), 'FM999999999.00') AS "Media Ciências Humanas",
    TO_CHAR(AVG(dpo.NU_NOTA_LC), 'FM999999999.00') AS "Media Linguagens e Códigos",
    TO_CHAR(AVG(dpo.NU_NOTA_MT), 'FM999999999.00') AS "Media Matemática",
    TO_CHAR(AVG(dr.NU_NOTA_REDACAO), 'FM999999999.00') AS "Media Redação"
FROM ENEM_STORAGE.DATA.FATO_ENEM fe
INNER JOIN ENEM_STORAGE.DATA.dim_prova_objetiva dpo ON dpo.ID_PROVA_OBJETIVA = fe.ID_PROVA_OBJETIVA
INNER JOIN ENEM_STORAGE.DATA.dim_redacao dr ON dr.ID_REDACAO = fe.ID_REDACAO
INNER JOIN ENEM_STORAGE.DATA.dim_participante dp ON dp.ID_PARTICIPANTE = fe.ID_PARTICIPANTE
GROUP BY dp.NU_ANO, dp.TP_COR_RACA
ORDER BY dp.NU_ANO DESC;

