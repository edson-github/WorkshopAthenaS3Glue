#!/usr/bin/env python3
"""
Script para executar consultas SQL no Amazon Athena.

Este script permite executar consultas na Amazon Athena usando
os dados que foram catalogados pelo AWS Glue após o processo ETL.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

import boto3
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("athena_query")

# Carrega variáveis de ambiente do arquivo .env se existir
load_dotenv()


def run_athena_query(query, database, s3_output, wait=True):
    """
    Executa consulta na Athena e retorna os resultados.
    
    Args:
        query (str): Consulta SQL a ser executada.
        database (str): Nome do banco de dados.
        s3_output (str): Local S3 para salvar os resultados.
        wait (bool): Se deve aguardar a conclusão da consulta.
        
    Returns:
        DataFrame: Resultados da consulta se wait=True, caso contrário, o ID da consulta.
    """
    try:
        logger.info(f"Executando consulta no banco de dados: {database}")
        logger.info(f"Salvando resultados em: {s3_output}")
        
        if wait:
            # Executa a consulta e aguarda o resultado
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=s3_output,
                ctas_approach=False,  # Não usa CTAS para consultas simples
            )
            logger.info(f"Consulta executada com sucesso. {len(df)} resultados retornados.")
            return df
        else:
            # Apenas inicia a consulta sem aguardar
            query_execution_id = wr.athena.start_query_execution(
                sql=query,
                database=database,
                s3_output=s3_output,
            )
            logger.info(f"Consulta iniciada com ID: {query_execution_id}")
            return query_execution_id
            
    except Exception as e:
        logger.error(f"Erro ao executar consulta: {e}")
        return None


def save_query_results(df, output_file, format="csv"):
    """
    Salva os resultados da consulta em um arquivo local.
    
    Args:
        df (DataFrame): DataFrame com os resultados.
        output_file (str): Nome do arquivo de saída.
        format (str): Formato do arquivo (csv, parquet, xlsx).
        
    Returns:
        bool: True se salvou com sucesso, False caso contrário.
    """
    if df is None or df.empty:
        logger.warning("Sem resultados para salvar")
        return False
        
    try:
        # Cria o diretório de saída se não existir
        os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
        
        format = format.lower()
        if format == "csv":
            df.to_csv(output_file, index=False)
        elif format == "parquet":
            df.to_parquet(output_file, index=False)
        elif format == "xlsx":
            df.to_excel(output_file, index=False)
        else:
            logger.error(f"Formato não suportado: {format}")
            return False
            
        logger.info(f"Resultados salvos em: {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao salvar resultados: {e}")
        return False
        

def check_query_status(query_execution_id):
    """
    Verifica o status de uma consulta na Athena.
    
    Args:
        query_execution_id (str): ID da execução da consulta.
        
    Returns:
        str: Status da consulta.
    """
    try:
        session = boto3.session.Session()
        athena_client = session.client('athena')
        
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        
        state = response['QueryExecution']['Status']['State']
        return state
    
    except Exception as e:
        logger.error(f"Erro ao verificar status da consulta: {e}")
        return "FAILED"


def wait_for_query_completion(query_execution_id, check_interval=1, max_attempts=100):
    """
    Aguarda a conclusão de uma consulta no Athena.
    
    Args:
        query_execution_id (str): ID da execução da consulta.
        check_interval (int): Intervalo entre verificações em segundos.
        max_attempts (int): Número máximo de tentativas.
        
    Returns:
        bool: True se a consulta concluiu com sucesso, False caso contrário.
    """
    logger.info(f"Aguardando conclusão da consulta {query_execution_id}...")
    attempts = 0
    
    while attempts < max_attempts:
        state = check_query_status(query_execution_id)
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            logger.info(f"Consulta concluída com status: {state}")
            return state == 'SUCCEEDED'
            
        logger.info(f"Consulta em execução (status: {state}). Aguardando {check_interval} segundos...")
        time.sleep(check_interval)
        attempts += 1
    
    logger.warning(f"Tempo limite excedido aguardando a consulta {query_execution_id}")
    return False


def get_query_results(query_execution_id, s3_output):
    """
    Obtém os resultados de uma consulta pela ID de execução.
    
    Args:
        query_execution_id (str): ID da execução da consulta.
        s3_output (str): Local S3 onde os resultados foram salvos.
        
    Returns:
        DataFrame: DataFrame pandas com os resultados da consulta.
    """
    try:
        df = wr.athena.read_sql_query_results(
            query_execution_id=query_execution_id,
            boto3_session=boto3.session.Session()
        )
        logger.info(f"Resultados recuperados com sucesso. {len(df)} registros.")
        return df
    except Exception as e:
        logger.error(f"Erro ao recuperar resultados da consulta: {e}")
        return None


def main():
    """Função principal que processa argumentos e executa consultas na Athena."""
    parser = argparse.ArgumentParser(description='Executar consultas SQL no Amazon Athena')
    
    parser.add_argument('--query', type=str, help='Consulta SQL a ser executada')
    parser.add_argument('--query-file', type=str, help='Arquivo contendo a consulta SQL')
    parser.add_argument('--database', type=str, help='Nome do banco de dados na Athena')
    parser.add_argument('--s3-output', type=str, help='Local S3 para salvar os resultados')
    parser.add_argument('--output-file', type=str, help='Arquivo local para salvar os resultados')
    parser.add_argument('--format', type=str, choices=['csv', 'parquet', 'xlsx'], default='csv',
                        help='Formato do arquivo de saída (default: csv)')
    parser.add_argument('--async', dest='is_async', action='store_true', 
                        help='Executar consulta de forma assíncrona')
    parser.add_argument('--query-id', type=str, help='ID de uma consulta existente para verificar status')
    
    args = parser.parse_args()
    
    # Definir valores padrão a partir de variáveis de ambiente
    database = args.database or os.getenv('GLUE_DATABASE', 'meu_database')
    s3_output = args.s3_output or os.getenv('ATHENA_RESULTS_BUCKET', 's3://seu-bucket-athena-results/')
    
    # Verificar se o S3 output termina com /
    if s3_output and not s3_output.endswith('/'):
        s3_output += '/'
    
    # Verificar se o S3 output começa com s3://
    if s3_output and not s3_output.startswith('s3://'):
        s3_output = f"s3://{s3_output}"
    
    # Verificar ou recuperar resultados de uma consulta existente
    if args.query_id:
        if wait_for_query_completion(args.query_id):
            df = get_query_results(args.query_id, s3_output)
            if df is not None and args.output_file:
                save_query_results(df, args.output_file, args.format)
        return 0
    
    # Obter a consulta SQL
    query = args.query
    if not query and args.query_file:
        try:
            with open(args.query_file, 'r') as f:
                query = f.read()
        except Exception as e:
            logger.error(f"Erro ao ler arquivo de consulta: {e}")
            return 1
    
    if not query:
        logger.error("É necessário fornecer uma consulta SQL via --query ou --query-file")
        return 1
    
    # Executar a consulta
    if args.is_async:
        query_id = run_athena_query(query, database, s3_output, wait=False)
        if query_id:
            print(f"Consulta iniciada com ID: {query_id}")
            print(f"Para verificar os resultados posteriormente, execute:")
            print(f"python workshop/athena_query.py --query-id {query_id}")
            return 0
        return 1
    else:
        df = run_athena_query(query, database, s3_output)
        if df is not None:
            # Exibir os primeiros registros
            print("\nPrimeiros 5 registros dos resultados:")
            print(df.head().to_string())
            
            # Salvar resultados se solicitado
            if args.output_file:
                save_query_results(df, args.output_file, args.format)
            
            return 0
        return 1


if __name__ == "__main__":
    sys.exit(main()) 