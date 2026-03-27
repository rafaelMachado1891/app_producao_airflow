from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
from urllib.parse import quote_plus

# Carregar variáveis de ambiente
load_dotenv()

def get_source_engine():
    """Retorna a engine de conexão com o banco de dados de origem (SQL Server)."""
    username = os.getenv("DB_USERNAME")
    password = quote_plus(os.getenv("DB_PASSWORD"))  # Codificar a senha para URL
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_NAME")
    return create_engine(
        f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )

def get_target_engine():
    """Retorna a engine de conexão com o banco de dados de destino (PostgreSQL)."""
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    NAME = os.getenv('NAME')
    USUARIO = os.getenv('USUARIO')
    SENHA = os.getenv('SENHA')
    return create_engine(f"postgresql://{USUARIO}:{SENHA}@{HOST}:{PORT}/{NAME}")

schema = os.getenv("SCHEMA")

target_engine = get_target_engine()
with target_engine.connect() as connection:
    connection.execution_options(autocommit=True)  # Ativa o autocommit
    connection.execute(text(f'TRUNCATE TABLE  "{schema}"."ORDERS" CASCADE'))
        
def execute_query(engine, query):
    """Executa uma consulta SQL no banco de dados e retorna um DataFrame com os resultados."""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def main():
    # Engines de conexão
    source_engine = get_source_engine()  # Banco de origem (SQL Server)
    target_engine = get_target_engine()  # Banco de destino (PostgreSQL)
    
    # Consulta SQL para extrair dados do banco de origem
    query = """
    WITH ORDENS_PRODUCAO AS (
        SELECT
            a.Numero AS NUMERO,
            a.Codigo AS CODIGO,
            a.Descr AS DESCRICAO,
            a.Quantidade AS QUANTIDADE,
            a.Saldo AS SALDO,
            FORMAT(a.Data_ET, 'dd/MM/yyyy') AS DATA_ENTREGA,
            FORMAT(b.Data_EM, 'dd/MM/yyyy') AS DATA_EMISSAO,
            CASE 
                WHEN c.Planejador = 1025 THEN 'CELULA-2'
                WHEN c.Planejador = 1020 THEN 'CELULA-1'
                WHEN c.Planejador = 1035 THEN 'CELULA-4'
                WHEN c.Planejador = 1030 THEN 'CELULA-5'
                ELSE 'NAO-INFORMADO'
            END AS CELULA,
            a.Situacao_MRP AS SITUACAO_MRP,
            a.Situacao AS SITUACAO_ORDEM
        FROM Ords2 a
        LEFT JOIN Ords1 b ON a.Numero = b.Numero
        LEFT JOIN Produtos c ON a.Codigo = c.Codigo
        WHERE 
            a.Situacao IN (0, 1, 2) AND
            a.Situacao_MRP <> 1 AND
            c.Planejador IN (1020, 1025, 1030, 1035) AND
            YEAR(b.Data_EM) >= 2021
    ),

    PRODUTOS_CTES AS (
        SELECT
            A.Codigo AS CODIGO,
            A.Descricao AS DESCRICAO,
            C.descricao AS LINHA,
            A.Referencia AS REFERENCIA,
            A.Complemento AS COMPLEMENTO,
            D.Descricao AS GRUPO,
            A.Ean13 AS CODIGO_BARRAS,
            A.Produtos_Volume AS QUANTIDADE_EMBALAGEM,
            B.Codigo_Fiscal AS CODIGO_FISCAL,
            A.Altura AS ALTURA_MASTER,
            A.Largura AS LARGURA_MASTER,
            A.Comprimento AS COMPRIMENTO_MASTER,
            A.Peso_Bruto AS PESO_CAIXA_MASTER,
            A.embalagemindividualaltura AS ALTURA_EMBALAGEM_INDIVIDUAL,
            A.embalagemindividuallargura AS LARGURA_EMBALAGEM_INDIVIDUAL,
            A.embalagemindividualcomprimento AS COMPRIMENTO_EMBALAGEM_INDIVIDUAL,
            A.embalagemindividualpeso AS PESO_EMBALAGEM_INDIVIDUAL,
            A.produtoaltura AS ALTURA_PRODUTO,
            A.produtolargura AS LARGURA_PRODUTO,
            A.produtocomprimento AS COMPRIMENTO_PRODUTO,
            A.Peso_Liquido AS PESO_PRODUTO,
            A.Amperagem AS LAMPADA,
            E.Caminho AS FOTO
        FROM Produtos A
        LEFT JOIN Class_Fiscal B ON B.Codigo = A.Classificacao_Fiscal
        LEFT JOIN marcas C ON C.codigo = A.Marca
        LEFT JOIN Grupos D ON A.Grupo = D.Codigo
        LEFT JOIN Fotos_Produtos E ON A.Codigo = E.Codigo
        WHERE 
            AXEntrada = 3 AND
            Situacao = 0
    )

    SELECT
        A.NUMERO,
        A.CODIGO,
        B.REFERENCIA,
        B.LINHA,
        B.GRUPO,
        A.QUANTIDADE,
        A.SALDO,
        A.DATA_EMISSAO,
        A.DATA_ENTREGA
    FROM ORDENS_PRODUCAO A
    JOIN PRODUTOS_CTES B ON A.CODIGO = B.CODIGO
    """

    # Extrair dados do banco de origem
    print("Extraindo dados do banco de origem (SQL Server)...")
    df = execute_query(source_engine, query)

    # Carregar dados no banco de destino
    print("Carregando dados no banco de destino (PostgreSQL)...")
    df.to_sql(
        name='ORDERS',          # Nome da tabela de destino
        con=target_engine,           # Conexão com o banco de destino
        schema=os.getenv('SCHEMA'),  # Schema do banco de destino
        if_exists='append',          # Adicionar dados à tabela existente
        index=False                  # Não incluir o índice do DataFrame
    )
    print("✅ Dados carregados com sucesso no banco de destino!")

if __name__ == "__main__":
    main()