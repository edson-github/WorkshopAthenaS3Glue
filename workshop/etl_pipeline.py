#!/usr/bin/env python3
"""
Pipeline ETL para processar dados CSV, converter para Parquet e catalogar com AWS Glue.

Este script executa um fluxo de trabalho ETL completo:
1. Extrai dados de arquivos CSV
2. Transforma os dados conforme necessário
3. Carrega os dados em formato Parquet no S3
4. Atualiza o catálogo de dados usando AWS Glue
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import awswrangler as wr
from dotenv import load_dotenv


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("etl_pipeline")

# Carrega variáveis de ambiente do arquivo .env se existir
load_dotenv()


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
        logger.info(f"Extraindo dados do arquivo: {source_file}")
        df = pd.read_csv(source_file)
        logger.info(f"Dados extraídos com sucesso. {len(df)} registros encontrados.")
        return df
    except Exception as e:
        logger.error(f"Erro na extração dos dados: {e}")
        return None


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
        logger.info(f"Enviando arquivo {file_path} para s3://{bucket}/{s3_key}")
        s3_client.upload_file(file_path, bucket, s3_key)
        logger.info(f"Arquivo enviado com sucesso")
    except Exception as e:
        logger.error(f"Erro no upload para S3: {e}")
        raise


def transform_data(df, partition_date=None):
    """
    Aplica transformações no DataFrame.
    
    Args:
        df (DataFrame): DataFrame pandas original.
        partition_date (datetime, optional): Data para particionamento.
        
    Returns:
        DataFrame: DataFrame pandas transformado.
    """
    if df is None or df.empty:
        logger.error("DataFrame vazio ou nulo. Não é possível transformar os dados.")
        return None
    
    try:
        logger.info("Iniciando transformação dos dados...")
        
        # Adicionar colunas de partição baseadas na data
        if partition_date is None:
            partition_date = datetime.now()
            
        df['ano'] = partition_date.strftime("%Y")
        df['mes'] = partition_date.strftime("%m")
        df['dia'] = partition_date.strftime("%d")
        
        # Remover duplicatas
        df = df.drop_duplicates()
        logger.info(f"Duplicatas removidas. Restaram {len(df)} registros.")
        
        # Tratar valores nulos (exemplo genérico)
        # Adapte conforme as colunas específicas do seu DataFrame
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = df[col].fillna(0)
            
        for col in df.select_dtypes(include=['object']).columns:
            if col not in ['ano', 'mes', 'dia']:  # Não limpar colunas de partição
                df[col] = df[col].fillna('desconhecido')
        
        # Converter tipos de dados (exemplo genérico)
        date_columns = [col for col in df.columns if 'data' in col.lower() or 'date' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                logger.warning(f"Não foi possível converter a coluna {col} para datetime")
        
        # Criar colunas derivadas (exemplo)
        if 'preco' in df.columns and 'quantidade' in df.columns:
            df['valor_total'] = df['preco'] * df['quantidade']
            logger.info("Coluna 'valor_total' criada com sucesso")
        
        logger.info(f"Transformação concluída. {len(df)} registros após transformação.")
        return df
    
    except Exception as e:
        logger.error(f"Erro durante a transformação dos dados: {e}")
        return None


def save_to_parquet(df, bucket, s3_path, partition_cols=None):
    """
    Salva DataFrame como arquivo Parquet no S3.
    
    Args:
        df (DataFrame): DataFrame pandas processado.
        bucket (str): Nome do bucket S3.
        s3_path (str): Caminho no S3 para salvar o arquivo Parquet.
        partition_cols (list, optional): Colunas para particionar os dados.
    """
    if df is None or df.empty:
        logger.error("DataFrame vazio ou nulo. Não é possível salvar como Parquet.")
        return False
    
    try:
        # Caminho completo no S3
        s3_uri = f"s3://{bucket}/{s3_path}"
        logger.info(f"Salvando dados em formato Parquet em {s3_uri}")
        
        # Configurar colunas de partição padrão se não especificadas
        if partition_cols is None:
            partition_cols = ['ano', 'mes', 'dia']
        
        # Verificar se as colunas de partição existem no DataFrame
        for col in partition_cols:
            if col not in df.columns:
                logger.warning(f"Coluna de partição {col} não existe no DataFrame. Não será usada para particionamento.")
                partition_cols.remove(col)
        
        # Salvar como Parquet
        wr.s3.to_parquet(
            df=df,
            path=s3_uri,
            dataset=True,
            partition_cols=partition_cols
        )
        
        logger.info(f"Dados salvos com sucesso em {s3_uri}")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao salvar dados no S3: {e}")
        return False


def run_glue_crawler(crawler_name):
    """
    Executa um Crawler do AWS Glue.
    
    Args:
        crawler_name (str): Nome do crawler a ser executado.
        
    Returns:
        bool: True se o crawler foi executado com sucesso, False caso contrário.
    """
    try:
        glue_client = boto3.client('glue')
        logger.info(f"Iniciando execução do crawler: {crawler_name}")
        
        # Verificar estado atual do crawler
        response = glue_client.get_crawler(Name=crawler_name)
        current_state = response['Crawler']['State']
        
        if current_state == 'RUNNING':
            logger.info(f"Crawler {crawler_name} já está em execução.")
            wait_for_crawler_completion(glue_client, crawler_name)
            return True
            
        # Iniciar o crawler
        glue_client.start_crawler(Name=crawler_name)
        
        # Aguardar conclusão
        return wait_for_crawler_completion(glue_client, crawler_name)
    
    except Exception as e:
        logger.error(f"Erro ao executar o crawler: {e}")
        return False


def wait_for_crawler_completion(glue_client, crawler_name, check_interval=30, max_attempts=20):
    """
    Aguarda a conclusão de um crawler do AWS Glue.
    
    Args:
        glue_client: Cliente do AWS Glue.
        crawler_name (str): Nome do crawler.
        check_interval (int): Intervalo entre verificações em segundos.
        max_attempts (int): Número máximo de tentativas.
        
    Returns:
        bool: True se o crawler concluiu com sucesso, False caso contrário.
    """
    logger.info(f"Aguardando conclusão do crawler {crawler_name}...")
    attempts = 0
    
    while attempts < max_attempts:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            
            if state != 'RUNNING':
                last_status = response['Crawler']['LastCrawl']['Status'] if 'LastCrawl' in response['Crawler'] else 'UNKNOWN'
                logger.info(f"Crawler {crawler_name} concluído com status: {last_status}")
                return last_status == 'SUCCEEDED'
                
            logger.info(f"Crawler ainda em execução. Aguardando {check_interval} segundos...")
            time.sleep(check_interval)
            attempts += 1
        
        except Exception as e:
            logger.error(f"Erro ao verificar estado do crawler: {e}")
            return False
    
    logger.warning(f"Tempo limite excedido aguardando o crawler {crawler_name}")
    return False


def run_etl_pipeline(csv_file_path, raw_bucket, processed_bucket, crawler_name, partition_date=None):
    """
    Executa o pipeline ETL completo.
    
    Args:
        csv_file_path (str): Caminho local do arquivo CSV.
        raw_bucket (str): Nome do bucket S3 para dados brutos.
        processed_bucket (str): Nome do bucket S3 para dados processados.
        crawler_name (str): Nome do crawler do AWS Glue.
        partition_date (datetime, optional): Data para particionamento.
        
    Returns:
        bool: True se o pipeline foi executado com sucesso, False caso contrário.
    """
    if partition_date is None:
        partition_date = datetime.now()
    
    # Verificar se o arquivo existe
    if not os.path.exists(csv_file_path):
        logger.error(f"Arquivo não encontrado: {csv_file_path}")
        return False
    
    # Extrair nome do arquivo e diretório
    file_path = Path(csv_file_path)
    file_name = file_path.stem
    
    # Variáveis de partição
    ano = partition_date.strftime("%Y")
    mes = partition_date.strftime("%m")
    dia = partition_date.strftime("%d")
    
    try:
        # 1. Extração: Ler dados CSV
        logger.info("1. Extraindo dados...")
        df = extract_data(csv_file_path)
        
        if df is None:
            return False
        
        # 2. Upload do CSV para S3 (raw data)
        logger.info("2. Enviando dados brutos para S3...")
        raw_s3_key = f"raw/{file_name}/{ano}/{mes}/{dia}/{file_name}.csv"
        upload_to_s3(csv_file_path, raw_bucket, raw_s3_key)
        
        # 3. Transformação: Processar dados
        logger.info("3. Transformando dados...")
        df = transform_data(df, partition_date)
        
        if df is None:
            return False
        
        # 4. Carga: Salvar como Parquet no S3
        logger.info("4. Salvando dados processados como Parquet...")
        parquet_s3_path = f"processed/{file_name}"
        success = save_to_parquet(
            df, 
            processed_bucket, 
            parquet_s3_path,
            partition_cols=['ano', 'mes', 'dia']
        )
        
        if not success:
            return False
        
        # 5. Executar o Crawler para atualizar o catálogo
        logger.info("5. Executando Crawler para atualizar o catálogo...")
        if not run_glue_crawler(crawler_name):
            logger.warning("Crawler não concluiu com sucesso. Verifique o console do AWS Glue.")
            return False
        
        logger.info("Pipeline ETL concluído com sucesso!")
        return True
    
    except Exception as e:
        logger.error(f"Erro durante a execução do pipeline ETL: {e}")
        return False


def main():
    """Função principal que processa argumentos e executa o pipeline ETL."""
    parser = argparse.ArgumentParser(description='ETL Pipeline para CSV -> Parquet -> S3 -> Glue -> Athena')
    
    parser.add_argument('--csv-file', type=str, required=True, 
                       help='Caminho para o arquivo CSV de entrada')
    parser.add_argument('--raw-bucket', type=str,
                       help='Nome do bucket S3 para dados brutos')
    parser.add_argument('--processed-bucket', type=str,
                       help='Nome do bucket S3 para dados processados')
    parser.add_argument('--crawler-name', type=str,
                       help='Nome do crawler AWS Glue')
    parser.add_argument('--date', type=str,
                       help='Data para particionamento (formato YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Definir valores padrão com base nas variáveis de ambiente ou valores fixos
    raw_bucket = args.raw_bucket or os.getenv('RAW_BUCKET', 'seu-bucket-raw-data')
    processed_bucket = args.processed_bucket or os.getenv('PROCESSED_BUCKET', 'seu-bucket-processed-data')
    crawler_name = args.crawler_name or os.getenv('GLUE_CRAWLER_NAME', 'meu-crawler-parquet')
    
    # Processar data de particionamento
    partition_date = None
    if args.date:
        try:
            partition_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            logger.error("Formato de data inválido. Use YYYY-MM-DD")
            return 1
    
    # Executar o pipeline
    success = run_etl_pipeline(
        args.csv_file,
        raw_bucket,
        processed_bucket,
        crawler_name,
        partition_date
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main()) 