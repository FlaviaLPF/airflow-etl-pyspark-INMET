
-- Tabela FATO_PREVISOES_DIA
CREATE SCHEMA IF NOT EXISTS analitic;
CREATE OR REPLACE TABLE analitic.fato_agg_previsoes_dia AS
SELECT
  f.WMO,  
  c.cidade_sk,
  f.DATA_MEDICAO,
  MIN(f.TEMPERATURA_C) AS temp_min_c,
  MAX(f.TEMPERATURA_C) AS temp_max_c,
  AVG(f.TEMPERATURA_C) AS temp_avg_c,
  SUM(f.PRECIPITACAO_MM) AS precip_total_mm,
  AVG(f.PRESSAO_ATM_KPA) AS pressao_avg_kpa,
  AVG(f.VENTO_MPS) AS vento_avg_mps,
  AVG(f.UMIDADE_PORCENTAGEM) AS umidade_avg_pct,
  COUNT(*) AS registros_horarios
FROM stage.fato_previsoes f
JOIN analitic.dim_cidade_atributos c ON f.WMO = c.WMO
GROUP BY f.WMO, c.cidade_sk , f.DATA_MEDICAO;