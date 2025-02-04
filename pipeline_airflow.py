import os
import logging
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import argparse
from pathlib import Path

class NorthwindETL:
    def __init__(self, postgres_conn_string, csv_path, execution_date=None):
        self.postgres_conn_string = postgres_conn_string
        self.csv_path = csv_path
        self.execution_date = execution_date or datetime.now().strftime('%Y-%m-%d')
        self.setup_logging()
        
    def setup_logging(self):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"logs/etl_{self.execution_date}.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging
        
    def get_all_tables(self):
        """Obtém todas as tabelas do banco de dados Northwind"""
        engine = create_engine(self.postgres_conn_string)
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        with engine.connect() as conn:
            tables = pd.read_sql(query, conn)
        return tables['table_name'].tolist()
    
    def extract_from_postgres(self):
        """Extrai todas as tabelas do PostgreSQL"""
        try:
            engine = create_engine(self.postgres_conn_string)
            tables = self.get_all_tables()
            
            for table in tables:
                self.logger.info(f"Extraindo tabela: {table}")
                query = f"SELECT * FROM {table}"
                df = pd.read_sql(query, engine)
                
                path = Path(f"data/postgres/{table}/{self.execution_date}")
                path.mkdir(parents=True, exist_ok=True)
                
                output_file = path / f"{table}.parquet"
                df.to_parquet(output_file)
                
            return True
        except Exception as e:
            self.logger.error(f"Erro ao extrair do Postgres: {str(e)}")
            return False
        
    def extract_from_csv(self):
        """Extrai dados do arquivo CSV"""
        try:
            df = pd.read_csv(self.csv_path)
            
            path = Path(f"data/csv/{self.execution_date}")
            path.mkdir(parents=True, exist_ok=True)
            
            output_file = path / "order_details.parquet"
            df.to_parquet(output_file)
            
            return True
        except Exception as e:
            self.logger.error(f"Erro ao extrair do CSV: {str(e)}")
            return False
            
    def check_extract_success(self):
        """Verifica se ambas as extrações foram bem-sucedidas"""
        postgres_files = list(Path(f"data/postgres").glob(f"*/{self.execution_date}/*.parquet"))
        csv_files = list(Path(f"data/csv/{self.execution_date}").glob("*.parquet"))
        
        return len(postgres_files) > 0 and len(csv_files) > 0
    
    def load_to_postgres(self):
        """Carrega dados para o PostgreSQL final"""
        if not self.check_extract_success():
            self.logger.error("Arquivos de extração não encontrados. Execute a etapa 1 primeiro.")
            return False
            
        try:
            engine = create_engine(self.postgres_conn_string)
            
            # Carregar todas as tabelas do Postgres
            postgres_path = Path(f"data/postgres")
            for table_path in postgres_path.glob("*"):
                table_name = table_path.name
                file_path = table_path / self.execution_date / f"{table_name}.parquet"
                
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    df.to_sql(f"{table_name}_final", engine, if_exists='replace', index=False)
            
            # Carregar dados do CSV
            csv_file = Path(f"data/csv/{self.execution_date}/order_details.parquet")
            if csv_file.exists():
                df = pd.read_parquet(csv_file)
                df.to_sql('order_details_final', engine, if_exists='replace', index=False)
                
            self.create_combined_view()
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar para o Postgres: {str(e)}")
            return False
            
    def create_combined_view(self):
        """Cria view combinando pedidos e detalhes"""
        engine = create_engine(self.postgres_conn_string)
        query = """
        CREATE OR REPLACE VIEW orders_complete AS
        SELECT o.*, d.*
        FROM orders_final o
        JOIN order_details_final d ON o.order_id = d.order_id;
        """
        with engine.connect() as conn:
            conn.execute(text(query))
            
    def export_results(self):
        """Exporta resultados da consulta final"""
        engine = create_engine(self.postgres_conn_string)
        query = "SELECT * FROM orders_complete"
        
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        df = pd.read_sql(query, engine)
        df.to_csv(f"results/orders_complete_{self.execution_date}.csv", index=False)
        df.to_json(f"results/orders_complete_{self.execution_date}.json", orient='records')

def main():
    parser = argparse.ArgumentParser(description='Northwind ETL Pipeline')
    parser.add_argument('--date', help='Data de execução (YYYY-MM-DD)', default=None)
    parser.add_argument('--postgres-conn', required=True, help='String de conexão PostgreSQL')
    parser.add_argument('--csv-path', required=True, help='Caminho do arquivo CSV')
    parser.add_argument('--step', choices=['extract', 'load', 'all'], default='all',
                       help='Etapa a ser executada')
    
    args = parser.parse_args()
    
    etl = NorthwindETL(args.postgres_conn, args.csv_path, args.date)
    
    if args.step in ['extract', 'all']:
        postgres_success = etl.extract_from_postgres()
        csv_success = etl.extract_from_csv()
        
        if not (postgres_success and csv_success):
            return
            
    if args.step in ['load', 'all']:
        if etl.load_to_postgres():
            etl.export_results()

if __name__ == "__main__":
    main()