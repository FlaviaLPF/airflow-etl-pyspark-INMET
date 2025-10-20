
-- Tabela CIDADE_KPI_MENSAL
--KPIs por cidade e mês: médias, anomalias, dias com precipitação.
------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE analitic.cidade_kpis_mensal AS
SELECT
  c.cidade_sk,
  dt.ANO,
  dt.MES,
  AVG(d.temp_avg_c) AS mensal_temp_media,
  MAX(d.temp_max_c) AS mensal_temp_max,
  SUM(d.precip_total_mm) AS mensal_precip_total,
  COUNT_IF(d.precip_total_mm > 0) AS dias_com_precip
FROM analitic.fato_agg_previsoes_dia d
JOIN analitic.dim_cidade_atributos c ON d.WMO = c.WMO
JOIN stage.dim_datas dt ON d.DATA_MEDICAO = dt.DATA_MEDICAO
GROUP BY c.cidade_sk, dt.ANO, dt.MES
;