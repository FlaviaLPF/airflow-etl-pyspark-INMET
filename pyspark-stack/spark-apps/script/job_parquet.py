from pyspark.sql import SparkSession
from pyspark.sql.functions import (
 min as spark_min, max as spark_max)
import pandas as pd
import numpy as np
import traceback
import unicodedata, re


# === Arquivos ===
CAMINHO_CSV = "hdfs://hdfs-namenode:9000/inmet_etl/input/"
CAMINHO_SAIDA_CIDADES = "hdfs://hdfs-namenode:9000/inmet_etl/stage/cidades.parquet"
CAMINHO_SAIDA_DATAS = "hdfs://hdfs-namenode:9000/inmet_etl/stage/datas.parquet"
CAMINHO_SAIDA_PREVISOES = "hdfs://hdfs-namenode:9000/inmet_etl/stage/previsoes.parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("CreateParquetFiles-testando")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )
    print("***Iniciando job de criacao parquets")
    
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path("hdfs://hdfs-namenode:9000/inmet_etl/stage")

    if not fs.exists(path):
        fs.mkdirs(path)


    path = sc._jvm.org.apache.hadoop.fs.Path(CAMINHO_CSV)

    arquivos_csv = [
    f.getPath().getName()
    for f in fs.listStatus(path)
    if f.isFile() and f.getPath().getName().lower().endswith(".csv")
    ]

    print(f">> Total de arquivos CSV encontrados: {len(arquivos_csv)}")
    print("Arquivos:", arquivos_csv)

    df_cidades_all = None
    df_previsoes_all = None
    lista_datas = []
    lista_previsoes = []
    
    for arquivo in arquivos_csv:
        try:
            caminho_arquivo = CAMINHO_CSV + arquivo

            print(f"Processando arquivo: {arquivo}")

                       
# ===  Le cabecalho-dados das cidades ===

           
            # --- Le cabecalho-dados das cidades com Spark e converte para pandas ---
            df_meta = (
                spark.read
                    .option("encoding", "ISO-8859-1")
                    .option("sep", ";")
                    .csv(caminho_arquivo, header=None)
                    .limit(8)              # pega so as 8 primeiras linhas
                    .toPandas()            # converte para pandas
            )
            
            #pegar o cabecalho que esta na primeira coluna das 8 primeiras linhas
            nomes  = df_meta.iloc[:, 0]\
                    .str.replace(":", "", regex=False)\
                    .str.lower()\
                    .tolist()
            
            #seleciona todos os valores da segunda coluna das 8 primeiras  linhas
            valores = df_meta.iloc[:, 1].tolist()            
            
          
            # Ajusta nomes de colunas especificas
            substituicoes = {"codigo (wmo)": "wmo", "data de fundacao": "data_fundacao"}
            nomes = [substituicoes.get(n, n) for n in nomes]

            #cria um dataframe com apenas uma linha de dados
            df_meta_pandas = pd.DataFrame([valores], columns=nomes)

            # Converte colunas numericas (latitude, longitude, altitude) no pandas antes de passar para spark
            for col_name in ['latitude', 'longitude', 'altitude']:
                if col_name in df_meta_pandas.columns:
                    # Substitui virgulas por pontos e converte para float
                    df_meta_pandas[col_name] = df_meta_pandas[col_name].str.replace(',', '.').astype(float)
                
            # Converte data_fundacao para datetime pandas
            if 'data_fundacao' in df_meta_pandas.columns:
                df_meta_pandas['data_fundacao'] = pd.to_datetime(
                df_meta_pandas['data_fundacao'], dayfirst=True, errors='coerce'
                )
            
            df_meta_pandas = df_meta_pandas.dropna(subset=['data_fundacao'])            
            df_meta_pandas['data_fundacao'] = df_meta_pandas['data_fundacao'].dt.strftime('%Y-%m-%d')

            # Cria DataFrame Spark a partir do pandas já com os tipos corretos
            df_meta_spark = spark.createDataFrame(df_meta_pandas)
            df_cidades_all = df_meta_spark if df_cidades_all is None else df_cidades_all.unionByName(df_meta_spark)

                     
         
# ==== Le restante do arquivo e salva DADOS METEREOLOGICOS ==== 
                      
            # le o CSV inteiro do HDFS como Spark DataFrame 
                                  
           
            df_text = spark.read.text(caminho_arquivo)                     
           
           # segunda parte do arquivo possui layout diferente (que comeca na linha 9)
           # 2) Transforma em RDD[String], remove ; e  descarta 8 primeiras linhas
            rdd = (
                df_text.rdd
                        .map(lambda r: r.value.rstrip(";"))
                        .zipWithIndex()
                        .filter(lambda t: t[1] >= 8)
                        .map(lambda t: t[0])
            )

                      
            # 2) Spark do CSV a partir do RDD ja "encurtado", com header na nona linha original
            df = (
                spark.read
                    .option("header", True)
                    .option("sep", ";")
                    .option("encoding", "UTF-8")
                    .option("nullValue", "None")
                    .option("treatEmptyValuesAsNulls", True)
                    .csv(rdd)                   
            )

            
            def sanitize(col: str) -> str:
                nfkd = unicodedata.normalize("NFKD", col)
                no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
                cleaned = re.sub(r"[^\w]+", "_", no_accents).strip("_")
                return cleaned.lower()

            novos = [sanitize(c) for c in df.columns]
            df = df.toDF(*novos)
            df = df.toPandas()   
                                

            def converter_coluna_float(df, nome_coluna_original, nome_coluna_nova=None):
    
                #Converte uma coluna de strings para float, tratando virgulas como separador decimal
                #e valores vazios como NaN e  renomea a coluna.

                #Parametros:
                #- df: DataFrame pandas
                #- nome_coluna_original: str
                #- nome_coluna_nova: str ou None (se None, sobrescreve a coluna original)
                #
    
                nome_coluna_nova = nome_coluna_nova or nome_coluna_original

                df[nome_coluna_nova] = (
                    df[nome_coluna_original]
                        .astype(str)
                        .str.strip()
                        .replace({'': np.nan,
                                    'None': np.nan,
                                    'nan':   np.nan})     
                        .str.replace(',', '.', regex=False)
                        .astype(float)
                )

                return df

            df = converter_coluna_float(df, "precipitacao_total_horario_mm", "precipitacao_mm")
            df = converter_coluna_float(df, "pressao_atmosferica_max_na_hora_ant_aut_mb","pressao_atm_kpa")
            df = converter_coluna_float(df, "temperatura_do_ar_bulbo_seco_horaria_c", "temperatura_c")
            df= converter_coluna_float(df, "vento_velocidade_horaria_m_s", "vento_mps")           
            df= converter_coluna_float(df,"umidade_relativa_do_ar_horaria","umidade_porcentagem")
            df = df.loc[:, ~df.columns.isna()]

            # Adiciona coluna WMO ao DataFrame Pandas
            idx_wmo = nomes.index("wmo")
            wmo_valor = str(valores[idx_wmo])
            df["wmo"] = wmo_valor
                        
            # Converte a coluna "Data" para datetime 
                

            df["data_medicao"] = pd.to_datetime(df["data"],dayfirst=True, errors='coerce')
            df["data_medicao"]  = df["data_medicao"] .dt.strftime('%Y-%m-%d') 

            df= df.dropna(subset=['data_medicao'])          
            

            # Adiciona a coluna "data_medicao" (como Series) na lista de datas
            lista_datas.append(df["data_medicao"])
            
            # Campos que serao usados salvos no previsoes.parquet
            colunas_finais = [
                "wmo",
                "data_medicao",
                "precipitacao_mm",
                "pressao_atm_kpa",
                "temperatura_c",
                "umidade_porcentagem",
                "vento_mps",
                ]

            
            df_dados = df[colunas_finais].copy()

            #colunas com valor nulo -devem ficar com zero
            df_dados = df_dados.fillna({
                "precipitacao_mm": 0,
                "pressao_atm_kpa": 0,
                "temperatura_c": 0,
                "umidade_porcentagem": 0,
                "vento_mps": 0
            })
            
            df_spark = spark.createDataFrame(df_dados)

            # Adiciona o DataFrame à lista
            lista_previsoes.append(df_spark)
                
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
            traceback.print_exc()
            return  
       
            
           
# === Salva CIDADES.PARQUET ===
    
    if df_cidades_all is not None:
        # Reordena e seleciona apenas as colunas desejadas para salvar no arquivo cidades.parquet
        colunas_existentes = ['regiao', 'uf', 'estacao', 'wmo', 'latitude', 'longitude', 'altitude','data_fundacao']
        df_cidades_all.select(*colunas_existentes).write.mode("overwrite").parquet(CAMINHO_SAIDA_CIDADES)
        print("***Arquivo cidades.parquet salvo.")
    else:
        print("***Nenhum dado de cidades foi coletado.")
     
   
 # === Salva PREVISOES.PARQUET ===
    if lista_previsoes:   
        print(f">> Total de blocos de previsão: {len(lista_previsoes)}")
        # Garante que todos sao Spark DataFrames antes de unir
        df_previsoes_all = lista_previsoes[0]
    
        for df in lista_previsoes[1:]:
            df_previsoes_all = df_previsoes_all.unionByName(df)
        # Salva o DataFrame final em Parquet
        df_previsoes_all.write.mode("overwrite").parquet(CAMINHO_SAIDA_PREVISOES)
        print("***Arquivo previsoes.parquet salvo.")
    else:
        print("***Nenhum dado de previsão foi coletado.")


# === Salva DATAS.PARQUET ===
    if lista_datas:
        # criar DataFrame pandas com coluna nomeada
        df_concatenado = pd.DataFrame(pd.concat(lista_datas).dropna(), columns=["data_medicao"])
        todas_datas_df = spark.createDataFrame(df_concatenado)        
        data_inicio = todas_datas_df.select(spark_min("data_medicao")).collect()[0][0]
        data_fim = todas_datas_df.select(spark_max("data_medicao")).collect()[0][0]
        datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')

        df_datas = pd.DataFrame({
            'data_medicao': datas,
            'dia': datas.day,
            'mes': datas.month,
            'ano': datas.year,
            'quartil': datas.quarter,
            'semana_do_ano': datas.isocalendar().week
        })
        df_datas['data_medicao'] = df_datas['data_medicao'].dt.strftime('%Y-%m-%d')
        df_datas_spark = spark.createDataFrame(df_datas)
        df_datas_spark.write.mode("overwrite").parquet(CAMINHO_SAIDA_DATAS)
        print("***Arquivo datas.parquet salvo.")
    else:
        print("***Nenhum dado de datas foi coletado.")



    spark.stop()

if __name__ == "__main__":
    main()
