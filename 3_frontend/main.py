from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

# Carregar variáveis de ambiente
load_dotenv(os.path.join("..", "1_src", ".env"))

# Verificar se as variáveis de ambiente foram carregadas corretamente
required_vars = ["HOST", "PORT", "NAME", "USUARIO", "SENHA"]
for var in required_vars:
    if os.getenv(var) is None:
        st.error(f"A variável de ambiente '{var}' não está definida no arquivo .env.")
        st.stop()  # Interrompe a execução do Streamlit

HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
NAME = os.getenv('NAME')
USUARIO = os.getenv('USUARIO')
SENHA = os.getenv('SENHA')
SCHEMA = os.getenv('SCHEMA')

# String de conexão com o banco de dados
DATABASE_URL = f"postgresql://{USUARIO}:{SENHA}@{HOST}:{PORT}/{NAME}"

try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()
    print("Conexão com o banco de dados estabelecida com sucesso!")
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.stop()  # Interrompe a execução do Streamlit

query_1 = """
SELECT * FROM public_gold.current_order_prevision
"""

query_2 = """
SELECT * FROM public_gold.orders_today
"""


query_3 = """
SELECT * FROM public_gold.quantity_produced
"""

query_4 = """
SELECT * FROM public_gold.orders_produced_by_week
WHERE current_year = 2025
"""

st.set_page_config(page_title="acompanhamento producao", layout="wide")

st.title('Apontamento de produção',)

# Executa a query e exibe a métrica
with engine.connect() as connection:
    resultado = connection.execute(text(query_3))
    row = resultado.fetchone()  # Pega a primeira linha do resultado
    total_produzido = row[0]  # Extrai o valor da primeira coluna
    
    total_produzido_int = int(total_produzido)

    # Exibe a métrica no Streamlit
    st.metric(label="Total Produzido", value=total_produzido_int)


st.subheader('Ordem em andamento')
with engine.connect() as connection:
     # Executa a primeira consulta
    result = connection.execute(text(query_1))
    df1 = pd.DataFrame(result.fetchall(), columns=result.keys())
    df1 = df1.reset_index(drop=True)
    st.dataframe(df1,hide_index=True)
    
st.subheader('Ordens produzidas hoje')
with engine.connect() as connection:
    # Executa a segunda consulta
    result = connection.execute(text(query_2))
    df2 = pd.DataFrame(result.fetchall(), columns=result.keys())
    #df2 = df2.reset_index(drop=True)
    st.dataframe(df2,hide_index=True)
    
with engine.connect() as connection:
    result = connection.execute(text(query_4))
    df4 = pd.DataFrame(result.fetchall(), columns=result.keys())

if 'quatity' in df4.columns:
    fig = px.line(df4,x='weekend_to_year', y='quatity',title='Quantidade por Semana', markers=True) # 
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        xaxis=dict(type="category", tickmode="linear", tickangle=-45))

    # Exibe no Streamlit
    st.plotly_chart(fig, use_container_width=True)
else:
    st.write("Coluna 'quatity' não encontrada no DataFrame.")