

-- Challenge second question
-- Reformatted for consumption
WITH metricas_horarias AS (
    -- Passo 1: Consolida a média de passageiros por hora para o mês de Maio
    SELECT 
        HOUR_SLICE AS hora_do_dia,
        AVG(avg_passenger_count) AS media_passageiros_hora,
        SUM(total_trips) AS volume_total_corridas
    FROM 
        ifood.gold.GLD_TXI_YEL_TRIP_MAY_ANALYTICS
    GROUP BY 
        1
),
analise_janela AS (
    -- Passo 2: Calcula a linha de base (média global) das 24 horas em Maio
    -- e computa o desvio/variância para cada hora específica
    SELECT 
        hora_do_dia,
        ROUND(media_passageiros_hora, 4) AS media_passageiros_hora,
        volume_total_corridas,
        ROUND(AVG(media_passageiros_hora) OVER(), 4) AS media_global_maio_horas,
        ROUND(media_passageiros_hora - AVG(media_passageiros_hora) OVER(), 4) AS desvio_da_media
    FROM 
        metricas_horarias
)
-- Passo 3: Retorna os insights finais com o status de comportamento do cliente
SELECT 
    hora_do_dia,
    media_passageiros_hora,
    media_global_maio_horas,
    desvio_da_media,
    CASE 
        WHEN desvio_da_media > 0 THEN '🟢 Densidade Superior à Média'
        WHEN desvio_da_media < 0 THEN '🔴 Densidade Inferior à Média'
        ELSE '🟡 Na Média Global'
    END AS status_comportamento_cliente
FROM 
    analise_janela
ORDER BY 
    hora_do_dia;