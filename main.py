import os
import requests
import json
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Função que você já usa para buscar contratos no Locavia
from Locavia.locavia import get_contratos_locavia  

# -------------------------------------------------------
# 1. CARREGA VARIÁVEIS DO .ENV
# -------------------------------------------------------

load_dotenv("config/.env")

API_URL_AUTH = "https://api.sandbox.sankhya.com.br/authenticate"
API_URL_DATA = "https://api.sandbox.sankhya.com.br/gateway/v1/mgecom/service.sbr?serviceName=CACSP.incluirAlterarFinanceiro&outputType=json"

API_CLIENT_ID = os.getenv("client_id")
API_CLIENT_SECRET = os.getenv("client_secret")
API_X_TOKEN = os.getenv("X-Token")

SANKHYA_HOST = os.getenv("HOST_SANKHYA")
SANKHYA_PORT = os.getenv("PORT_SANKHYA")
SANKHYA_DB = os.getenv("DB_SANKHYA")
SANKHYA_USER = os.getenv("USER_SANKHYA")
SANKHYA_PASS = os.getenv("PASS_SANKHYA")
SANKHYA_DRIVER = "ODBC Driver 18 for SQL Server"

current_access_token = None
token_expiry_time = 0
TEMP_NUFIN = "0"

# -------------------------------------------------------
# 2. AUTENTICAÇÃO API
# -------------------------------------------------------

def get_new_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": API_CLIENT_ID,
        "client_secret": API_CLIENT_SECRET
    }

    headers = {
        "accept": "application/x-www-form-urlencoded",
        "X-Token": API_X_TOKEN,
        "content-type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(API_URL_AUTH, data=payload, headers=headers)
        response.raise_for_status()

        token_data = response.json()

        global current_access_token, token_expiry_time
        current_access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 3600)

        token_expiry_time = time.time() + expires_in - 60  # renova 1 min antes

        return current_access_token

    except Exception as e:
        print(f"❌ Erro ao obter token: {e}")
        return None


def get_current_token():
    global current_access_token, token_expiry_time

    if current_access_token is None or time.time() >= token_expiry_time:
        print("⏳ Token expirado ou inexistente. Renovando…")
        token = get_new_token()
        if token:
            print("✅ Token renovado.")
        return token

    return current_access_token


# -------------------------------------------------------
# 3. CONEXÃO COM BANCO SANKHYA
# -------------------------------------------------------

def get_sankhya_db_engine():
    try:
        connection_string = (
            f"mssql+pyodbc://{SANKHYA_USER}:{SANKHYA_PASS}"
            f"@{SANKHYA_HOST}:{SANKHYA_PORT}/{SANKHYA_DB}"
            f"?driver={SANKHYA_DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
        )
        return create_engine(connection_string, fast_executemany=True)

    except Exception as e:
        print(f"❌ Erro ao criar engine SQL: {e}")
        return None


def consulta_nufin_sankhya(engine, codigo_contrato):

    query = text(f"""
        select
            t.NUFIN,
            CONVERT(VARCHAR(10), t.DTVENC, 103) as DTVENC
        from SANKHYA.TGFFIN t
        where t.NUMNOTA = '{codigo_contrato}'
        order by t.NUFIN asc
        offset 0 rows fetch next 1 rows only
    """)

    try:
        df = pd.read_sql(query, engine)

        if not df.empty:
            return str(df.iloc[0]["NUFIN"]), df.iloc[0]["DTVENC"]

        return None, None

    except Exception as e:
        print(f"⚠️ Erro consultando NUFIN: {e}")
        return None, None


# -------------------------------------------------------
# 4. ENVIO PARA A API SANKHYA
# -------------------------------------------------------

def enviar_transacao_sankhya(access_token, nufin, dt_venc, codigo_contrato, itens_pagamento):

    print(f"⚙️  Processando Contrato {codigo_contrato} | NUFIN: {nufin}")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Clona itens
    itens_para_api = [i.copy() for i in itens_pagamento]

    # 🔥 Ajuste obrigatório: DTVENC deve existir em TODOS os itens
    for item in itens_para_api:
        item["DTVENC"]["$"] = dt_venc

    payload = {
        "serviceName": "CACSP.incluirAlterarFinanceiro",
        "requestBody": {
            "nota": {
                "nufin": str(nufin),
                "isParcelamentoVariosTipTit": "true",
                "itens": {
                    "item": itens_para_api
                }
            }
        }
    }

    try:
        response = requests.post(API_URL_DATA, headers=headers, json=payload)
        data = response.json()

        if data.get("status") == "1":
            print("✅ Sucesso!")
            print(json.dumps(data, indent=4))
            return True

        print(f"❌ Erro da API: {data.get('statusMessage')}")
        print(json.dumps(data, indent=4))
        return False

    except Exception as e:
        print(f"❌ ERRO enviando transação: {e}")
        return False


# -------------------------------------------------------
# 5. MAIN
# -------------------------------------------------------

if __name__ == "__main__":

    print("--- 🔑 Autenticando com a API Sankhya ---")
    access_token = get_current_token()

    if not access_token:
        print("❌ Falha na autenticação. Saindo.")
        exit()

    engine = get_sankhya_db_engine()
    if not engine:
        print("❌ Falha ao conectar no SQL Server Sankhya.")
        exit()

    contratos = get_contratos_locavia()
    if not contratos:
        print("⚠️ Nenhum contrato encontrado no Locavia.")
        exit()

    print("\n--- 🚀 Iniciando processamento dos contratos ---\n")

    sucessos = 0

    for contrato in contratos:
        codigo = contrato["CodigoContrato"]
        itens = contrato["ItensPagamento"]

        nufin, dtvenc = consulta_nufin_sankhya(engine, codigo)

        if not nufin:
            nufin = TEMP_NUFIN
            dtvenc = time.strftime("%d/%m/%Y")

        ok = enviar_transacao_sankhya(access_token, nufin, dtvenc, codigo, itens)

        if ok:
            sucessos += 1

    print("\n--- 🏁 PROCESSAMENTO FINALIZADO ---")
    print(f"Total de contratos: {len(contratos)}")
    print(f"Sucesso: {sucessos}")
    print(f"Falhas: {len(contratos) - sucessos}")

    engine.dispose()
    print("Conexão com o DB finalizada.")

