import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import extract
import transform
from time import sleep

# Carregar variáveis de ambiente
load_dotenv()

HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
NAME = os.getenv('NAME')
USUARIO = os.getenv('USUARIO')
SENHA = os.getenv('SENHA')
SCHEMA = os.getenv('SCHEMA')

# String de conexão com o banco de dados
DATABASE_URL = f"postgresql://{USUARIO}:{SENHA}@{HOST}:{PORT}/{NAME}"
engine = create_engine(DATABASE_URL)

def carregar_dados(carga_completa=False):
    """
    Carrega os dados no banco de dados.
    :param carga_completa: Se True, faz uma carga completa (substitui todos os dados). Se False, faz carga incremental.
    """
    # Extrair dados da API do SharePoint
    token = extract.obter_token()
    
    if carga_completa:
        df = extract.obter_lista_itens(token, primeira_carga=True)  # Carrega todos os dados
        ultima_datahora = None  # Carga completa, então ignoramos o filtro
    else:
        ultima_datahora = transform.obter_ultima_data(engine)  # Obtém a última data de carga
        df = extract.obter_lista_itens(token, primeira_carga=True)  # Carrega dados incrementais

    # Transformar os dados passando a última data correta
    df = transform.tratar_dados(df, ultima_datahora)

    # Carregar os dados no banco de dados
    if not df.empty:
        if carga_completa:
            # Limpa a tabela antes de fazer a carga completa
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                    text(f"TRUNCATE TABLE {SCHEMA}.apontamento")
                )
            print("✅ Tabela dropada para carga completa.")

        # Insere os dados na tabela
        df.to_sql('apontamento', engine, if_exists='append', index=False, schema=SCHEMA)
        print("✅ Dados carregados com sucesso!")
    else:
        print("⚠️ Nenhum novo dado para carregar.")
        
if __name__ == "__main__":
  #   while True:
    
        CARGA_COMPLETA = True  # Altere para False para carga incremental
        df = carregar_dados(carga_completa=CARGA_COMPLETA)
        # sleep(1800)
            