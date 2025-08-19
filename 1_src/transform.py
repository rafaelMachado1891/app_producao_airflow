import pandas as pd
from sqlalchemy import text

def obter_ultima_data(engine):
    """Obtém a última data e o maior ID do banco de dados."""
    query = text('SELECT MAX("MODIFIED") FROM apontamento')
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
    ultima_data = result if result else None
    return  ultima_data

import pandas as pd

def tratar_dados(df, ultima_datahora=None):
    """Processa e filtra os dados extraídos da API."""
    df.columns = df.columns.str.upper()

    df["DATAHORA"] = pd.to_datetime(df["DATA"] + " " + df["HORA"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    df["DATAHORASAIDA"] = pd.to_datetime(df["DATASAIDA"] + " " + df["HORASAIDA"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    df["ID"] = pd.to_numeric(df["ID"], errors="coerce")

    print(f"Tipo de dado da coluna MODIFIED antes da conversão: {df['MODIFIED'].dtype}")

    # Extrair o valor correto de ultima_datahora se for uma tupla
    if isinstance(ultima_datahora, tuple):
        ultima_datahora = ultima_datahora[0]

    print(f"Tipo de dado de ultima_datahora após conversão: {type(ultima_datahora)}, valor: {ultima_datahora}")

    if ultima_datahora:
        df = df.loc[df["MODIFIED"] > ultima_datahora].copy()

    df["ORDEMPRODUCAO"] = df["ORDEMPRODUCAO"].astype(str).str.replace("O", "").str.replace("#1#1O", "").str.replace("#1#1", "").str.replace('#1#2', '')
    df["TIPOMOVIMENTO"] = df["TIPOMOVIMENTO"].astype(str).str.replace('í', 'i').str.upper()
    df["TIPOSAIDA"] = df["TIPOSAIDA"].astype(str).str.upper()
    df["CELULA"] = df["CELULA"].astype(str).str.upper()
    df["QUANTIDADE"] = pd.to_numeric(df["QUANTIDADE"], errors="coerce")
    df = df.copy()
    df["DIFERENCATEMPO"] = (df["DATAHORASAIDA"] - df["DATAHORA"]).dt.total_seconds() / 3600

    limite_quantidade = 10000
    df_filtrado = df.loc[
        (df["DIFERENCATEMPO"] <= 24) &
        (df["QUANTIDADE"].isna() | (df["QUANTIDADE"] <= limite_quantidade)) &
        (df["ORDEMPRODUCAO"].str.len() <= 7)
    ].copy()

    mask = (df_filtrado["DATAHORA"].dt.time < pd.to_datetime("12:00:00").time()) & \
           (df_filtrado["DATAHORASAIDA"].dt.time > pd.to_datetime("13:00:00").time())
    df_filtrado.loc[mask, "DIFERENCATEMPO"] -= 1

    df_registros_atuais = df.loc[df["QUANTIDADE"].isna() & df["DATAHORASAIDA"].isna()].copy()
    df_final = pd.concat([df_filtrado, df_registros_atuais], ignore_index=True)

    print(f"Tipo de dado da coluna MODIFIED após conversão: {df_final['MODIFIED'].dtype}")

    df_final = df_final[["ORDEMPRODUCAO", "QUANTIDADE", "DATAHORA", "DATAHORASAIDA", "DIFERENCATEMPO", "ID", "MODIFIED", "CREATED"]]
    return df_final