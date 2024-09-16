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
    """
    Função principal que coordena o processo de extração e carregamento de dados.

    - Lista os arquivos CSV no diretório especificado.
    - Para cada arquivo CSV, executa a extração, processamento e carregamento dos dados em lotes.
    - Os dados processados são convertidos para o formato Parquet e carregados em um bucket S3.

    A função utiliza a função interna `source()` para obter a lista de arquivos e a função interna `extract()`
    para processar e carregar cada arquivo individualmente.
    """

    
    def source():
        """
        Obtém a lista de arquivos CSV disponíveis no diretório de dados.

        - Verifica o diretório especificado e lista os arquivos encontrados.

        Returns:
            list: Lista de nomes de arquivos CSV encontrados no diretório.
        """
        
        data_path = '/usr/local/airflow/include/sc'
        files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
        print(f"Arquivos encontrados: {files}")
        return files
    
    
    @task
    def extract(file_path):

        """
        Extrai e carrega dados do arquivo CSV especificado para um bucket S3.

        - Conecta ao banco de dados DuckDB.
        - Cria uma tabela temporária a partir do arquivo CSV.
        - Conta o número total de linhas no arquivo CSV e divide o processamento em lotes.
        - Para cada lote, lê os dados, converte para o formato Parquet e carrega no bucket S3.

        Args:
            file_path (str): O caminho relativo para o arquivo CSV a ser processado.
        """

        
        con = duckdb.connect()

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

    data_source = source()
    for file_path in data_source[1:]:
        extract(file_path)
        
extract_load()
