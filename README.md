Overview
========

FONTE DE DADOS:
- https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem

# Data Pipeline ELT com Astronomer Python SDK, DuckDB, Airbyte, DBT e Snowflake

Este projeto demonstra uma pipeline de dados utilizando tecnologias modernas de engenharia de dados. O fluxo de trabalho abrange desde a extração e armazenamento de dados brutos até a transformação e análise dos dados no Snowflake. As tecnologias utilizadas incluem o Astronomer Python SDK para Apache Airflow, DuckDB, Airbyte, DBT e Snowflake.

## Tecnologias Utilizadas

- **Astronomer Python SDK**: Orquestração e gerenciamento de workflows Airflow.
- **DuckDB**: Conversão de dados CSV para Parquet.
- **Airbyte**: Extração e carga de dados.
- **DBT**: Transformação de dados.
- **Snowflake**: Data Warehouse para armazenamento e análise.

## Estrutura do Projeto

1. **Extração e Armazenamento:**
   - Dados em formato CSV são extraídos e convertidos para Parquet usando DuckDB.
   - Os arquivos Parquet são carregados em um bucket S3.

2. **Carga e Transformação:**
   - Airbyte é utilizado para extrair dados do bucket S3 e carregá-los no Snowflake.
   - DBT é utilizado para transformar os dados no Snowflake.

3. **Geração de Insights:**
   - Utilização de ferramentas de BI para análise e visualização dos dados transformados.

## Requisitos

- **Python 3.8+**: Para executar scripts Python e instalar pacotes.
- **Astronomer Python SDK**: Para configurar e gerenciar o Airflow.
- **DuckDB**: Para conversão de dados.
- **Airbyte**: Para movimentação e carga de dados.
- **DBT**: Para transformação de dados.
- **Snowflake**: Para Data Warehouse.
- **Conta AWS**: Para o armazenamento no S3.

## Configuração

### 1. Clonar o Repositório

```bash
git clone https://github.com/lobobranco96/mds-data-pipeline.git
cd repositorio
