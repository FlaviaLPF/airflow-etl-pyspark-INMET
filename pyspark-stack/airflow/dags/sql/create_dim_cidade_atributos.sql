
-- Tabela DIM_CIDADE_ATRIBUTOS
--Dimensão de cidades enriquecida com chave surrogate e geografia tipo GEOGRAPHY (opcional).
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE analitic.dim_cidade_atributos AS
SELECT
  CONCAT(
  TRIM(UPPER(COALESCE(WMO,''))),
  '-',
  TRIM(UPPER(COALESCE(UF,''))),
  '-',
  TRIM(UPPER(COALESCE(ESTACAO,'')))) AS cidade_sk,
  WMO,
  UF,
  ESTACAO,
  REGIAO,
  LATITUDE,
  LONGITUDE,
  ALTITUDE,
  DATA_FUNDACAO
FROM stage.dim_cidades
;
