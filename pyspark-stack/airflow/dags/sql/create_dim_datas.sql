--Criar schema stage (caso ainda não exista)
CREATE SCHEMA IF NOT EXISTS stage;

-- Tabela DIM_DATAS
CREATE OR REPLACE TABLE stage.dim_datas (
    DATA_MEDICAO DATE ,      
    DIA INTEGER,
    MES INTEGER,
    ANO INTEGER,
    QUARTIL INTEGER,
    SEMANA_DO_ANO INTEGER
);

