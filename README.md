Overview
========

FONTE DE DADOS:
- https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem

# Data Pipeline ELT com DuckDB, Astro Python SDK, Snowflake e DBT

Este projeto demonstra uma pipeline de dados utilizando tecnologias modernas de engenharia de dados. O fluxo de trabalho abrange desde a extração e armazenamento de dados brutos até a transformação e análise dos dados no Snowflake. As tecnologias utilizadas incluem o Astro Python SDK para Apache Airflow, DuckDB, DBT e Snowflake.

![arquitetura](https://github.com/user-attachments/assets/8c6ecb96-0b95-4ca7-856d-d510f4614691)

## Tecnologias Utilizadas

- **Astronomer**: Orquestração e gerenciamento de workflows Airflow.
- **DuckDB**: Conversão de dados CSV para Parquet.
- **Astro Python SDK**: Extração e carga de dados.
- **DBT**: Transformação de dados.
- **Snowflake**: Data Warehouse para armazenamento e análise.

## Estrutura do Projeto

1. **Extração e Armazenamento:**
   - Dados em formato CSV são extraídos e convertidos para Parquet usando DuckDB.
   - Os arquivos Parquet são carregados em um bucket S3.

2. **Carga e Transformação:**
   - Astro Python SDK é utilizado para extrair dados do bucket S3 e carregá-los no Snowflake.
   - DBT é utilizado para transformar os dados no Snowflake.

3. **Geração de Insights:**
   - Utilização de ferramentas de BI para análise e visualização dos dados transformados.

## Requisitos

- **Python 3.8+**: Para executar scripts Python e instalar pacotes.
- **Docker**: É importante que para o funcionamento do Duckdb e a finalização da tarefa, é preciso que configure 12GB de memoria ao container.
- **Astronomer**: Para configurar e gerenciar o Airflow.
- **DuckDB**: Para conversão de dados.
- **Astro Python SDK**: Para movimentação e carga de dados.
- **DBT**: Para transformação de dados.
- **Snowflake**: Para Data Warehouse.
- **Conta AWS**: Para o armazenamento no S3.

## Configuração

### 1. Clonar o Repositório

```bash
git clone https://github.com/lobobranco96/mds-data-pipeline.git
cd repositorio
