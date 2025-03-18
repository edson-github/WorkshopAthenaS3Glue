# Workshop: ETL Pipeline com Python, S3, Glue e Athena

Este repositório contém instruções e código para implementar um pipeline ETL completo utilizando Python para processamento de dados (CSV para Parquet), Amazon S3 para armazenamento, AWS Glue para catalogação e Amazon Athena para consultas SQL.

## Índice

1. [Pré-requisitos](#pré-requisitos)
2. [Arquitetura da Solução](#arquitetura-da-solução)
3. [Configuração do Ambiente](#configuração-do-ambiente)
   - [Com Poetry](#configuração-com-poetry)
   - [Com pip/venv](#configuração-com-pipvenv)
4. [Extração (Extract)](#extração-extract)
5. [Transformação (Transform)](#transformação-transform)
6. [Carga (Load)](#carga-load)
7. [Catalogação com AWS Glue](#catalogação-com-aws-glue)
8. [Consultas com Amazon Athena](#consultas-com-amazon-athena)
9. [Automação do Pipeline](#automação-do-pipeline)
10. [Troubleshooting](#troubleshooting)

## Pré-requisitos

- Conta AWS com permissões para:
  - Amazon S3
  - AWS Glue
  - Amazon Athena
- Python 3.8 ou superior
- Poetry (recomendado) ou pip/venv
- AWS CLI configurado
- boto3 (SDK AWS para Python)
- pandas, pyarrow (para manipulação de dados e conversão para Parquet)

## Arquitetura da Solução

```
+----------------+     +-------------------+     +----------------+
|                |     |                   |     |                |
| Dados CSV      +---->+ Processamento     +---->+ Parquet no S3  |
| (Origem)       |     | Python            |     |                |
|                |     |                   |     |                |
+----------------+     +-------------------+     +------+---------+
                                                        |
                                                        v
+----------------+     +-------------------+     +------+---------+
|                |     |                   |     |                |
| Visualização   |<----+ Consultas SQL     |<----+ AWS Glue       |
| dos Resultados |     | (Amazon Athena)   |     | Crawler        |
|                |     |                   |     |                |
+----------------+     +-------------------+     +----------------+
```

## Configuração do Ambiente

### Configuração com Poetry

Poetry é uma ferramenta moderna para gerenciamento de dependências e empacotamento em Python. É a maneira recomendada para configurar este projeto.

#### 1. Instalação do Poetry

Se você ainda não tem o Poetry instalado:

```bash
# Linux/macOS/WSL
curl -sSL https://install.python-poetry.org | python3 -

# Windows (PowerShell)
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

#### 2. Configuração do projeto

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/workshop-athena-s3-glue.git
cd workshop-athena-s3-glue

# Instalar dependências com Poetry
poetry install

# Ativar o ambiente virtual
poetry shell
```

#### 3. Configuração das variáveis de ambiente

```bash
# Copiar o arquivo de exemplo
cp .env.example .env

# Editar o arquivo .env com suas configurações
nano .env  # ou use seu editor preferido
```

#### 4. Executando o pipeline ETL

```bash
# Com Poetry ativado
poetry run etl --csv-file dados/vendas.csv

# Ou com argumentos personalizados
poetry run etl --csv-file dados/vendas.csv --raw-bucket meu-bucket-raw --processed-bucket meu-bucket-processed --crawler-name meu-crawler
```

### Configuração com pip/venv

Se preferir usar o método tradicional com pip e venv:

```bash
# Criar e ativar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instalar pacotes necessários
pip install boto3 pandas pyarrow awswrangler python-dotenv

# Copiar o arquivo de exemplo de configuração
cp .env.example .env

# Editar o arquivo .env com suas configurações
nano .env  # ou use seu editor preferido
```

### Configuração da AWS CLI

```bash
aws configure
# Forneça suas credenciais AWS, região padrão e formato de saída
```

### Criação de Buckets S3

```bash
# Bucket para dados brutos (CSV)
aws s3 mb s3://seu-bucket-raw-data

# Bucket para dados processados (Parquet)
aws s3 mb s3://seu-bucket-processed-data

# Bucket para resultados da Athena
aws s3 mb s3://seu-bucket-athena-results
```

## Extração (Extract)

### Código Python para Extrair Dados de Origem (exemplo)

```python
import pandas as pd

def extract_data(source_file):
    """
    Extrai dados de um arquivo CSV.
    
    Args:
        source_file (str): Caminho para o arquivo CSV.
        
    Returns:
        DataFrame: DataFrame pandas com os dados extraídos.
    """
    try:
        # Leitura do arquivo CSV
        df = pd.read_csv(source_file)
        print(f"Dados extraídos com sucesso. {len(df)} registros encontrados.")
        return df
    except Exception as e:
        print(f"Erro na extração dos dados: {e}")
        return None
```

### Upload de Arquivos CSV para o S3

```python
import boto3

def upload_to_s3(file_path, bucket, s3_key):
    """
    Faz upload de um arquivo para o S3.
    
    Args:
        file_path (str): Caminho local do arquivo.
        bucket (str): Nome do bucket S3.
        s3_key (str): Caminho de destino no S3.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, s3_key)
        print(f"Arquivo {file_path} enviado para s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Erro no upload para S3: {e}")
```

## Transformação (Transform)

### Código Python para Transformar Dados

```python
def transform_data(df):
    """
    Aplica transformações no DataFrame.
    
    Args:
        df (DataFrame): DataFrame pandas original.
        
    Returns:
        DataFrame: DataFrame pandas transformado.
    """
    # Exemplo de transformações
    # 1. Remover duplicatas
    df = df.drop_duplicates()
    
    # 2. Tratar valores nulos
    df = df.fillna({
        'coluna_numerica': 0,
        'coluna_texto': 'desconhecido'
    })
    
    # 3. Converter tipos de dados
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'])
    
    # 4. Criar colunas derivadas
    if 'preco' in df.columns and 'quantidade' in df.columns:
        df['valor_total'] = df['preco'] * df['quantidade']
    
    print(f"Transformação concluída. {len(df)} registros após transformação.")
    return df
```

## Carga (Load)

### Salvando Dados em Formato Parquet no S3

```python
import awswrangler as wr

def save_to_parquet(df, bucket, s3_path):
    """
    Salva DataFrame como arquivo Parquet no S3.
    
    Args:
        df (DataFrame): DataFrame pandas processado.
        bucket (str): Nome do bucket S3.
        s3_path (str): Caminho no S3 para salvar o arquivo Parquet.
    """
    try:
        # Caminho completo no S3
        s3_uri = f"s3://{bucket}/{s3_path}"
        
        # Salvar como Parquet
        wr.s3.to_parquet(
            df=df,
            path=s3_uri,
            dataset=True,
            partition_cols=['ano', 'mes']  # Opcional: particionamento
        )
        
        print(f"Dados salvos com sucesso em {s3_uri}")
    except Exception as e:
        print(f"Erro ao salvar dados no S3: {e}")
```

## Catalogação com AWS Glue

### 1. Criação de um Crawler do AWS Glue via Console

1. Acesse o console AWS e navegue até o serviço AWS Glue
2. No painel lateral, clique em "Crawlers" 
3. Clique em "Add crawler"
4. Siga o assistente de configuração:
   - Nome do crawler
   - Fonte de dados: S3
   - Inclua o caminho para os dados em Parquet: `s3://seu-bucket-processed-data/`
   - Defina um banco de dados de destino no Catálogo de Dados
   - Configure a programação de execução
   - Finalize e execute o crawler

### 2. Criação de um Crawler via AWS CLI

```bash
aws glue create-crawler \
    --name "meu-crawler-parquet" \
    --role "AWSGlueServiceRole-CrawlerRole" \
    --database-name "meu_database" \
    --targets '{"S3Targets": [{"Path": "s3://seu-bucket-processed-data/"}]}'
```

### 3. Executar o Crawler

```bash
aws glue start-crawler --name "meu-crawler-parquet"
```

## Consultas com Amazon Athena

### 1. Configuração da Athena via Console AWS

1. Acesse o console AWS e navegue até o serviço Amazon Athena
2. Configure o local de resultados de consulta: `s3://seu-bucket-athena-results/`
3. No painel lateral, selecione o banco de dados criado pelo crawler do Glue
4. Você verá as tabelas descobertas pelo crawler

### 2. Exemplos de Consultas SQL

```sql
-- Consulta básica
SELECT * FROM "meu_database"."nome_tabela" LIMIT 10;

-- Consulta com filtros
SELECT 
  coluna1, 
  coluna2,
  SUM(valor_total) as total
FROM "meu_database"."nome_tabela"
WHERE ano = '2023'
GROUP BY coluna1, coluna2
ORDER BY total DESC;

-- Consultando dados particionados
SELECT 
  COUNT(*) as contagem,
  ano,
  mes
FROM "meu_database"."nome_tabela"
GROUP BY ano, mes
ORDER BY ano, mes;
```

### 3. Salvando Resultados de Consultas

```python
import boto3
import pandas as pd
import awswrangler as wr

# Executar consulta Athena via Python
def run_athena_query(query, database, s3_output):
    """
    Executa consulta na Athena e retorna os resultados.
    
    Args:
        query (str): Consulta SQL a ser executada.
        database (str): Nome do banco de dados.
        s3_output (str): Local S3 para salvar os resultados.
        
    Returns:
        DataFrame: Resultados da consulta.
    """
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=s3_output
        )
        print(f"Consulta executada com sucesso. {len(df)} resultados retornados.")
        return df
    except Exception as e:
        print(f"Erro ao executar consulta: {e}")
        return None
```

## Automação do Pipeline

### 1. Script Python para Pipeline Completo

Crie um arquivo `etl_pipeline.py` com o seguinte conteúdo:

```python
import pandas as pd
import boto3
import awswrangler as wr
import time
from datetime import datetime

# Configurações
RAW_BUCKET = "seu-bucket-raw-data"
PROCESSED_BUCKET = "seu-bucket-processed-data"
ATHENA_RESULTS_BUCKET = "seu-bucket-athena-results"
GLUE_DATABASE = "meu_database"
CRAWLER_NAME = "meu-crawler-parquet"

def run_etl_pipeline(csv_file_path, partition_date=None):
    """
    Executa o pipeline ETL completo.
    
    Args:
        csv_file_path (str): Caminho local do arquivo CSV.
        partition_date (datetime, optional): Data para particionamento.
    """
    if partition_date is None:
        partition_date = datetime.now()
    
    ano = partition_date.strftime("%Y")
    mes = partition_date.strftime("%m")
    dia = partition_date.strftime("%d")
    
    # Nome do arquivo sem extensão
    file_name = csv_file_path.split("/")[-1].split(".")[0]
    
    # 1. Extração: Ler dados CSV
    print("1. Extraindo dados...")
    df = pd.read_csv(csv_file_path)
    
    # 2. Upload do CSV para S3 (raw data)
    print("2. Enviando dados brutos para S3...")
    raw_s3_key = f"raw/{file_name}/{ano}/{mes}/{dia}/{file_name}.csv"
    s3_client = boto3.client('s3')
    s3_client.upload_file(csv_file_path, RAW_BUCKET, raw_s3_key)
    
    # 3. Transformação: Processar dados
    print("3. Transformando dados...")
    # Adicionar colunas de partição
    df['ano'] = ano
    df['mes'] = mes
    df['dia'] = dia
    
    # Aplicar outras transformações conforme necessário
    # ...
    
    # 4. Carga: Salvar como Parquet no S3
    print("4. Salvando dados processados como Parquet...")
    parquet_s3_path = f"processed/{file_name}"
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{PROCESSED_BUCKET}/{parquet_s3_path}",
        dataset=True,
        partition_cols=['ano', 'mes', 'dia']
    )
    
    # 5. Executar o Crawler para atualizar o catálogo
    print("5. Executando Crawler para atualizar o catálogo...")
    glue_client = boto3.client('glue')
    glue_client.start_crawler(Name=CRAWLER_NAME)
    
    # 6. Aguardar a conclusão do Crawler
    print("Aguardando conclusão do Crawler...")
    while True:
        response = glue_client.get_crawler(Name=CRAWLER_NAME)
        state = response['Crawler']['State']
        if state != 'RUNNING':
            break
        print("Crawler ainda em execução. Aguardando 30 segundos...")
        time.sleep(30)
    
    print("Pipeline ETL concluído com sucesso!")

if __name__ == "__main__":
    # Exemplo de uso
    run_etl_pipeline("dados/vendas.csv")
```

### 2. Configuração de Execução Programada via AWS CloudWatch Events

1. Crie uma função Lambda que execute o pipeline
2. Configure um evento CloudWatch para acionar a função Lambda em intervalos regulares

## Troubleshooting

### Problemas Comuns e Soluções

1. **Erro ao acessar o S3**
   - Verifique as permissões IAM
   - Confirme se o bucket existe e o nome está correto

2. **Crawler não encontra os dados**
   - Verifique o caminho S3 configurado no crawler
   - Confirme se os arquivos Parquet foram salvos corretamente

3. **Erro nas consultas Athena**
   - Verifique a sintaxe SQL
   - Confirme se a tabela existe no catálogo Glue
   - Verifique se o local de saída dos resultados da Athena está configurado

4. **Problemas de conversão para Parquet**
   - Verifique se há tipos de dados incompatíveis
   - Confirme se as bibliotecas pyarrow estão instaladas corretamente

5. **Erros de permissão**
   - Verifique se a política IAM inclui permissões para:
     - s3:GetObject, s3:PutObject
     - glue:StartCrawler, glue:GetCrawler
     - athena:StartQueryExecution, athena:GetQueryExecution

## Próximos Passos

- Adicionar monitoramento e alertas
- Implementar testes automatizados
- Configurar políticas de retenção de dados
- Implementar controle de versão dos dados
- Migrar para um modelo de Delta Lake ou Iceberg para suporte a transações ACID
