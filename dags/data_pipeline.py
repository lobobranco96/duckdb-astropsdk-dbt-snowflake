from airflow.decorators import dag, task
from datetime import datetime

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

import boto3
import os
import duckdb

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

SNOWFLAKE_CONN = "snowflake_default"
AWS_CONN = "aws_default"



@dag(
    start_date=datetime(2024, 1, 1),
    #schedule="@daily",
    catchup=False,
    default_args={"owner": "Renato", "retries": 10},
    tags=["ASTRO", "aws", "dbt"]
)
def data_pipeline():

        ############## LOCAL PARA O S3 ################
    @task
    def extract_load(file_path):

        # Crie um cursor para execução das consultas
        data_path = '/usr/local/airflow/include/sc'
        full_path = f"{data_path}/{file_path}"

        file_name = file_path.replace('.csv', '')

        con = duckdb.connect()
        cursor = con.cursor()
        query = f"""
        INSTALL httpfs;
        LOAD httpfs;
                    CREATE SECRET secret1 (
            TYPE S3,
            KEY_ID '{ACCESS_KEY}',
            SECRET '{SECRET_KEY}',
            REGION 'us-east-1'
        );
        CREATE TABLE enem_data AS SELECT * FROM read_csv_auto('{full_path}', ignore_errors=True);
        COPY enem_data TO 's3://raw-data-for-snowflake/{file_name}.parquet';
        """

        cursor.execute(query)
        print(f"Arquivo {file_name} adicionado ao bucket 'raw-data-for-snowflake'.")
        con.close()


    def source():
        data_path = '/usr/local/airflow/include/sc'
        files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
        print(f"Arquivos encontrados: {files}")
        return files


    """ Source retorna uma lista dos nomes dos arquivos encontrados dentro da pasta include, que servirá como parametro para serem utilizados dentra da função extract_load, que le os arquivos csv com o read_auto_csv e envia em formato parquet para o bucket s3 otimizando o armazenamento. """

    data_source = source()
    for file_path in data_source[1:]:
        extract_load(file_path)

    ###########################################


    def listar_arquivos_bucket():
        s3 = boto3.client('s3', region_name='us-east-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

        response = s3.list_objects_v2(Bucket='raw-data-for-snowflake', Prefix='')

        file_names = []
        if 'Contents' in response:
            for obj in response['Contents']:
                file_names.append(obj['Key'])

        print("Nomes dos arquivos encontrados:", file_names)
        return file_names  # Retorna a lista de arquivos

    @task
    def s3_to_snowflake(file_name):
        s3_to_snowflake_task = aql.load_file(
            task_id=f"{file_name}",
            input_file=File(path=f"s3://raw-data-for-snowflake/{file_name}.parquet", conn_id=AWS_CONN),
            output_table=Table(name="RAWENEMDATA", conn_id=SNOWFLAKE_CONN)
        )

        s3_to_snowflake_task  # Retorna a tarefa para o controle de execução

    # Chama a função para listar arquivos
    lista_arquivos_s3 = listar_arquivos_bucket()

    # Itera sobre a lista de arquivos e chama a tarefa para cada um
    for file_name in lista_arquivos_s3:
        s3_to_snowflake(file_name)

    

data_pipeline()


    
