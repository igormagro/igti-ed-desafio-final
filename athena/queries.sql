-- Questão 1
SELECT AVG(renda) FROM "db-igti"."pnadc"

-- Questão 2
SELECT AVG(renda) FROM "db-igti"."pnadc"
WHERE uf = 'Distrito Federal'

-- Questão 3
SELECT 
    AVG(renda) 
FROM "db-igti"."pnadc" p
LEFT JOIN  "db-igti"."mesoregions" m
    ON p.uf = m.ufname
WHERE m.ufregiaonome = 'Sudeste'

-- Questão 4
SELECT 
    uf,
    AVG(renda) as renda
FROM "db-igti"."pnadc"
GROUP BY uf
ORDER BY renda ASC LIMIT 1

-- Questão 5
SELECT 
    uf,
    AVG(anosesco) as anosesco
FROM "db-igti"."pnadc"
GROUP BY uf
ORDER BY anosesco DESC LIMIT 1

-- Questão 6
-- Não é possível responder pois não há homens na base

-- Questão 7
SELECT 
    AVG(anosesco) as anosesco
FROM "db-igti"."pnadc"
WHERE uf = 'Paraná' AND idade BETWEEN 25 AND 30

-- Questão 8
SELECT 
    AVG(renda) as renda
FROM "db-igti"."pnadc" p 
LEFT JOIN  "db-igti"."mesoregions" m
    ON p.uf = m.ufname
WHERE 
    m.ufregiaonome = 'Sul'
    AND idade BETWEEN 25 AND 35
    AND trab = 'Pessoas na força de trabalho'
    
-- Questão 9
SELECT (
    (
        SELECT
            SUM(p.renda)
        FROM "db-igti"."pnadc" p 
        WHERE p.uf = 'Minas Gerais'
    )/(
        SELECT 
            COUNT(DISTINCT id) 
        FROM "db-igti"."mesoregions" m
        WHERE m.ufsigla = 'MG'
    )
)

SELECT COUNT(DISTINCT id) FROM "db-igti"."mesoregions" m
WHERE m.ufsigla = 'MG'

-- Questão 10
SELECT 
   AVG(p.renda)
FROM "db-igti"."pnadc" p
-- LEFT JOIN  "db-igti"."mesoregions" m
--     ON p.uf = m.ufname
WHERE 
    p.uf IN ('Amazonas','Pará','Acre','Roraima','Rondônia','Amapá','Tocantins')
    graduacao = 'Sim'
    --AND m.ufregiaonome = 'Norte'
    AND idade BETWEEN 25 AND 35
    AND cor IN ('Preta', 'Parda')