import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import logging

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_URL = os.getenv("SITE_URL")
LIST_NAME = os.getenv("LIST_NAME")

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

def obter_token():
    """Obtém o token de acesso do SharePoint."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    try:
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"Erro ao obter token: {e}")
        raise

def obter_lista_itens(access_token, primeira_carga=True, ultima_datahora=None):
    """Obtém os dados da API do SharePoint e retorna um DataFrame."""
    GRAPH_API_URL = f"https://graph.microsoft.com/v1.0/sites/ditalcombr.sharepoint.com:/sites/Dital-ApontamentoProducao:/lists/{LIST_NAME}/items?$expand=fields"

    if not primeira_carga and ultima_datahora:
        data_filtro = ultima_datahora.strftime("%Y-%m-%dT%H:%M:%SZ")
        GRAPH_API_URL += f"&$filter=fields/Modified gt '{data_filtro}'"

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    all_items = []
    try:
        while GRAPH_API_URL:
            response = requests.get(GRAPH_API_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_items.extend(data.get("value", []))
            GRAPH_API_URL = data.get("@odata.nextLink")

        print(f"✅ Total de itens obtidos: {len(all_items)}")

        df = pd.DataFrame([item["fields"] for item in all_items]) if all_items else pd.DataFrame()

        if not df.empty and 'Modified' in df.columns:
            df["Modified"] = pd.to_datetime(df["Modified"], format="%Y-%m-%dT%H:%M:%SZ", utc=True)
        
        print(f"Tipo de dado da coluna MODIFIED: {df['Modified'].dtype}")
        return df
    except Exception as e:
        print(f"Erro ao obter dados do SharePoint: {e}")
        raise
    
    
if __name__ == "__main__":
    token = obter_token()

    # Definir se é primeira carga ou incremental (ajuste conforme necessário)
    PRIMEIRA_CARGA = True # Altere para True se quiser carregar tudo

    df = obter_lista_itens(token, primeira_carga=PRIMEIRA_CARGA)
    print(df.head())
    
    