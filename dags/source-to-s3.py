from airflow.decorators import dag, task
from pendulum import datetime
import os
import duckdb


ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["airbyte", "aws", "dbt"],
)
def extract_load():

    @task
    def extract(file_path):
        con = duckdb.connect()

        # Crie um cursor para execução das consultas
        data_path = '/usr/local/airflow/include/sc'
        full_path = f"{data_path}/{file_path}"
        # Contar o número total de linhas no CSV
        query = f"""
        CREATE OR REPLACE TEMPORARY TABLE temp_table AS
        SELECT *
        FROM read_csv_auto('{full_path}', ignore_errors=true)
        """
        con.execute(query)

        total_linhas = con.execute("SELECT COUNT(*) FROM temp_table").fetchone()[0]
        print(f'Total de linhas no CSV: {total_linhas}')
        #return total_linhas
        tamanho_batch = total_linhas / 10

        file_name = file_path.replace('.csv', '')

        for y, offset in enumerate(range(0, total_linhas, int(tamanho_batch))):
            # Defina a consulta para ler o lote
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
            CREATE TABLE enem_data AS SELECT * FROM read_csv_auto('{full_path}', ignore_errors=True)
            LIMIT {tamanho_batch} OFFSET {offset};
            COPY enem_data TO 's3://raw-data-for-snowflake/{file_name}{y}.parquet';
            """

            # Execute a consulta e obtenha o DataFrame
            cursor.execute(query)
            print(f"Arquivo {y} adicionado ao bucket.")
            con.close()


    def source():
        data_path = '/usr/local/airflow/include/sc'
        files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
        print(f"Arquivos encontrados: {files}")
        return files

    data_source = source()
    for file_path in data_source[1:]:
        extract(file_path)
        
extract_load()
