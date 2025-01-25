import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Configurações do banco de dados PostgreSQL
DB_HOST = 'localhost'  
DB_USER = 'postgres'
DB_PASSWORD = '1022'
DB_NAME = 'northwind'
DB_PORT = '5432'

# Configurações do arquivo CSV
CSV_PATH = r'C:\Users\Elbia Simone\Desktop\desafio de código\code-challenge\data\order_details.csv'  

# Criar conexão com o banco de dados
def get_postgres_connection():
    try:
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para extrair dados de uma tabela do PostgreSQL
def extract_postgres_data(table_name):
    engine = get_postgres_connection()
    if engine:
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            print(f"Erro ao extrair dados da tabela {table_name}: {e}")
            return None
    return None

# Função para salvar dados no disco local
def save_to_local(data, source_type, table_name=None):
    # Obter a data atual no formato YYYY-MM-DD
    today = datetime.today().strftime('%Y-%m-%d')
    
    # Criar o caminho da pasta com base na fonte e tabela
    if source_type == "postgres":
        folder = f"data/postgres/{table_name}/{today}/"
    elif source_type == "csv":
        folder = f"data/csv/{today}/"
    else:
        print("Tipo de fonte inválido!")
        return

    # Criar a pasta, se não existir
    os.makedirs(folder, exist_ok=True)

    # Criar o nome do arquivo
    file_name = f"{table_name or 'csv_data'}.csv"  # Nome do arquivo para CSV ou PostgreSQL
    file_path = os.path.join(folder, file_name)

    # Salvar o DataFrame como arquivo CSV
    try:
        data.to_csv(file_path, index=False)
        print(f"Dados salvos em: {file_path}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")

# Extrair dados do PostgreSQL e salvar no disco
def postgres_pipeline():
    tables = ['customers', 'orders', 'products']  # Tabelas do banco de dados
    for table in tables:
        data = extract_postgres_data(table)
        if data is not None:
            save_to_local(data, "postgres", table)

# Extrair dados do CSV e salvar no disco
def csv_pipeline():
    try:
        data = pd.read_csv(CSV_PATH)  # Ler o arquivo CSV
        save_to_local(data, "csv")  # Salvar no disco
    except Exception as e:
        print(f"Erro ao processar o arquivo CSV: {e}")

# Executar o pipeline
if __name__ == "__main__":
    print("Iniciando o pipeline...")
    postgres_pipeline()  # Processar dados do PostgreSQL
    csv_pipeline()  # Processar dados do CSV
    print("Pipeline concluído!")
