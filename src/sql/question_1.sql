

-- Challenge first question
-- Reformatted for consumption
WITH faturamento_mensal AS (
    -- Etapa 1: Organiza o faturamento real por mês
    SELECT 
        cast(TIME_SLICE as int) AS numero_mes,
        CASE cast(TIME_SLICE as int)
            WHEN 1 THEN '1. Jan'
            WHEN 2 THEN '2. Fev'
            WHEN 3 THEN '3. Mar'
            WHEN 4 THEN '4. Abr'
            WHEN 5 THEN '5. Mai'
        END AS nome_mes,
        round(total_amount, 2) AS faturamento_mes
    FROM 
        ifood.gold.GLD_TXI_YEL_TRIP_MONTHLY
),
analise_global AS (
    -- Etapa 2: Calcula a média global dos meses usando Window Function
    -- e faz a diferença matemática na mesma linha
    SELECT 
        numero_mes,
        nome_mes,
        faturamento_mes,
        round(avg(faturamento_mes) OVER(), 2) AS media_historica_mensal,
        round(faturamento_mes - avg(faturamento_mes) OVER(), 2) AS desvio_da_media
    FROM 
        faturamento_mensal
)
-- Etapa 3: Entrega o resultado final com o status de performance
SELECT 
    nome_mes,
    faturamento_mes,
    media_historica_mensal,
    desvio_da_media,
    CASE 
        WHEN desvio_da_media > 0 THEN '🟢 Acima da Média'
        WHEN desvio_da_media < 0 THEN '🔴 Abaixo da Média'
        ELSE '🟡 Na Média'
    END AS status_performance
FROM 
    analise_global
ORDER BY 
    numero_mes;