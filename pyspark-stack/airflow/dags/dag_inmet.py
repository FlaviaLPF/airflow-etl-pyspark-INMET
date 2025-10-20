from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from pathlib import Path


# carrega dados de arquivos Parquet do HDFS para tabelas do Snowflake

def load_parquet_dir_to_snowflake(hdfs_dir, nome_tabela):
   # from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
    from snowflake.connector.pandas_tools import write_pandas
    import pandas as pd
    import json
    from io import BytesIO
    from pyarrow import parquet as pq
    from airflow.hooks.base import BaseHook
    import snowflake.connector

    conn = BaseHook.get_connection("snowflake_default")
    extra = json.loads(conn.extra)
    database = extra.get("database", "TEMPOINMET")  
    schema = extra.get("schema", "STAGE")

    ctx = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=extra.get('account'),
        warehouse=extra.get('warehouse'),
        database=database,
        schema=schema,
        role=extra.get('role')
    )

    cs = ctx.cursor()
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")    


        
    try:
        # apenas logging do contexto para confirmação
        try:
            cs.execute("SELECT current_database(), current_schema(), current_warehouse(), current_role()")
            print("Sessão Snowflake:", cs.fetchone())
        except Exception as e:
            print("Não foi possível consultar contexto (não bloqueante):", e)

        # leitura HDFS
        hdfs = WebHDFSHook(webhdfs_conn_id="hdfs_default").get_conn()
        try:
            items = hdfs.list(hdfs_dir)
        except Exception as e:
            print("ERRO listando HDFS:", e)
            return

        parquet_files = [f for f in items if f.endswith(".parquet") and not f.startswith("_")]
        print("DEBUG: parquet_files:", parquet_files)
        if not parquet_files:
            print(f">> Nenhum arquivo parquet encontrado em {hdfs_dir}")
            return

        parts = []
        for fname in parquet_files:
            path = hdfs_dir.rstrip("/") + "/" + fname
            print("Lendo:", path)
            try:
                with hdfs.read(path) as fh:
                    buf = fh.read()
                table = pq.read_table(BytesIO(buf))
                parts.append(table.to_pandas())
                print(f"  Lido {len(parts[-1])} linhas de {fname}")
            except Exception as e:
                print(" ERRO lendo", path, "->", e)

        if not parts:
            print(">> Nenhuma parte lida do HDFS")
            return

        df = pd.concat(parts, ignore_index=True)
        df.columns = df.columns.str.upper()
        print("DEBUG: df.shape =", df.shape)
        if df.empty:
            print(">> DataFrame vazio. Nada a carregar.")
            return

              
        # grava com write_pandas usando a conexao retornada pelo hook
        table_name_upper = nome_tabela.upper()
        success, nchunks, nrows, _ = write_pandas(ctx, df, table_name=table_name_upper,schema=schema,database=database)
        print("write_pandas:", success, "nrows:", nrows)
        if not success:
            raise RuntimeError("write_pandas retornou False")
    finally:
        try:
            cs.close()
        except Exception:
            pass
        try:
            ctx.close()
        except Exception:
            pass

# Ler os arquivos SQL antes de criar o DAG / operators
sql_create_analitic_previsoes = (Path(__file__).parent / "sql" / "create_analitic_fato_agg_previsoes_dia.sql").read_text(encoding="utf-8")
sql_create_cidade_kpis = (Path(__file__).parent / "sql" / "create_analitic_cidade_kpis_mensal.sql").read_text(encoding="utf-8")
sql_create_dim_cidade_atributos = (Path(__file__).parent / "sql" / "create_dim_cidade_atributos.sql").read_text(encoding="utf-8")

with DAG(
    dag_id="pipeline_inmet",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start_process')
    end = EmptyOperator(task_id='end_process')
    start_dwh = EmptyOperator(task_id='start_dwh')
    create_external_tables = EmptyOperator(task_id='create_external_tables')
    load_external_tables= EmptyOperator(task_id='load_external_tables') 
    

    env = "dev"
    create_parquets = BashOperator(
        task_id="create_parquets",
        bash_command=f"bash /opt/spark-apps/shell/shell_job_parquet.sh {env}"
    )

        
    create_dim_cidades = SnowflakeOperator(
        task_id='create_dim_cidades',
        snowflake_conn_id='snowflake_default',
        sql='sql/create_dim_cidades.sql',
        do_xcom_push=False
    )

    create_dim_datas = SnowflakeOperator(
        task_id='create_dim_datas',
        snowflake_conn_id='snowflake_default',
        sql='sql/create_dim_datas.sql',
        do_xcom_push=False
    )
    
    create_fato_previsoes = SnowflakeOperator(
        task_id='create_fato_previsoes',
        snowflake_conn_id='snowflake_default',
        sql='sql/create_fato_previsoes.sql',
        do_xcom_push=False
    )

    # Carregamentos via PythonOperator usando HDFS
    
    load_dim_cidades = PythonOperator(
    task_id="load_cidades",
    python_callable=load_parquet_dir_to_snowflake,
    op_kwargs={
        "hdfs_dir": "/inmet_etl/stage/cidades.parquet",
        "nome_tabela": "dim_cidades"
    },
    )

    load_dim_datas = PythonOperator(
        task_id="load_datas",
        python_callable=load_parquet_dir_to_snowflake,
        op_kwargs={
            "hdfs_dir": "/inmet_etl/stage/datas.parquet",
            "nome_tabela": "dim_datas"          
        },
    )
    
    load_fato_previsoes = PythonOperator(
        task_id="load_previsoes",
        python_callable=load_parquet_dir_to_snowflake,
        op_kwargs={
            "hdfs_dir": "/inmet_etl/stage/previsoes.parquet",
            "nome_tabela": "fato_previsoes"            
        },
    )    

    
    create_analitic_previsoes_dia = SnowflakeOperator(
        task_id='create_analitic_previsoes_dia',
        snowflake_conn_id='snowflake_analitic',
        sql=sql_create_analitic_previsoes,
        autocommit=True,
        do_xcom_push=False
    )

    create_analitic_cidade_kpis_mensal = SnowflakeOperator(
        task_id='create_analitic_cidade_kpis_mensal',
        snowflake_conn_id='snowflake_analitic',
        sql=sql_create_cidade_kpis,
        autocommit=True,
        do_xcom_push=False
    )

    create_dim_cidade_atributos = SnowflakeOperator(
        task_id='create_dim_cidade_atributos',
        snowflake_conn_id='snowflake_analitic',
        sql=sql_create_dim_cidade_atributos,
        autocommit=True,
        do_xcom_push=False
    )


     # Orquestracao
    start  \
    >> create_parquets \
    >> create_external_tables \
    >> [create_dim_cidades, create_dim_datas, create_fato_previsoes] \
    >> load_external_tables \
    >> [load_dim_cidades,load_dim_datas,load_fato_previsoes] \
    >> start_dwh \
    >> create_dim_cidade_atributos \
    >> create_analitic_previsoes_dia \
    >> create_analitic_cidade_kpis_mensal \
    >> end
    
    