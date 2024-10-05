from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from astro.options import SnowflakeLoadOptions

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from pathlib import Path
import os

DBT_PATH = "/usr/local/airflow/dags/dbt"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"


ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

BUCKET_NAME = "raw-data-for-snowflake"
PROCESSED_DATA = "processed-data"
FILE_NAME = "merged_raw_enem_data_19_20_21_22_23"

DATA_PATH = '/usr/local/airflow/include/sc'
LOCAL_DB_PATH = '/usr/local/airflow/include/sc/enem_database.db'

SNOWFLAKE_CONN = "snowflake_default"
AWS_CONN = "aws_default"


profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

# Models dbt
project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"
)

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["duckdb", "aws", "dbt", "snowflake"],
)
def elt_datapipeline():
    """
    Função principal que coordena o processo de extração e carregamento de dados.
    - Atráves do DuckDb, Le todos os arquivos csvs dentro da include/sc e armazena em um .db connectado ao Duckdb
    - Os dados processados são convertidos para o formato Parquet e carregados em um bucket S3.

    """
    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    @task
    def duckdb_data_ingestion():
        import duckdb
        conn = duckdb.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()
        #Configuração com o S3
        config = f"""
            INSTALL httpfs;
            LOAD httpfs;
            CREATE SECRET secret1 (
                TYPE S3,
                KEY_ID '{ACCESS_KEY}',
                SECRET '{SECRET_KEY}',
                REGION 'us-east-1'
            );"""
        cursor.execute(config)
        print("Configuracao concluida.")

        cursor.execute("DROP TABLE IF EXISTS enem_table;")
        cursor.execute(f"""CREATE OR REPLACE TABLE enem_table AS SELECT * FROM read_csv_auto('{DATA_PATH}/*.csv', ignore_errors=True);""")
        print("Tabela criada.")

        cursor.execute(f"""
        COPY enem_table TO 's3://{BUCKET_NAME}/{PROCESSED_DATA}/{FILE_NAME}.parquet' (FORMAT 'parquet');
        """)
        print("Dados enviados com sucesso.")
        cursor.close()
        print("Cursor encerrado.")
        conn.close()
        print("Banco de dados encerrado.")
        cursor.close()


    load_options = SnowflakeLoadOptions(
    copy_options={
        "MATCH_BY_COLUMN_NAME": "CASE_INSENSITIVE"
    })
    ######################### ASTRO PYTHON SDK
    s3_path = f"s3://{BUCKET_NAME}/{PROCESSED_DATA}/{FILE_NAME}.parquet"
    s3_to_snowflake_task = aql.load_file(
            task_id="load_enem_data",
            input_file=File(path=f"{s3_path}", conn_id=AWS_CONN),
            output_table=Table(name="MERGED_ENEMDATA", conn_id=SNOWFLAKE_CONN),
            if_exists="replace",
            use_native_support=True,
            load_options=load_options,
            columns_names_capitalization="original"
        )
    
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    duckdb = duckdb_data_ingestion()

    init >> duckdb >> s3_to_snowflake_task >> dbt_running_models >> finish

elt_datapipeline()