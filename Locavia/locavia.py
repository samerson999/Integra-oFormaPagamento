import os
import requests
import time
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------
# Carregar variáveis de ambiente
# -------------------------------------------------------
load_dotenv("config/.env")

# Variáveis do SQL Server
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST2")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT")
SQLSERVER_DB   = os.getenv("SQLSERVER_DB")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS")

# Variáveis da API Sankhya
API_URL_AUTH = "https://api.sandbox.sankhya.com.br/authenticate"
API_CLIENT_ID = os.getenv("client_id")
API_CLIENT_SECRET = os.getenv("client_secret")
API_X_TOKEN = os.getenv("X-Token") # O token fixo ou secundário

# Variáveis de controle do Token
current_access_token = None
# Inicializa com 0 para forçar a primeira autenticação
token_expiry_time = 0 
driver = "ODBC Driver 18 for SQL Server"

# -------------------------------------------------------
# Funções de Autenticação e Refresh da API Sankhya
# -------------------------------------------------------

def get_new_token():
    """
    Solicita um novo 'access_token' para a API Sankhya.
    Usa 'client_id' e 'client_secret' para autenticar.
    """
    print("⏳ Solicitando novo token de acesso Sankhya...")
    
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
        response.raise_for_status() # Lança exceção para erros HTTP

        token_data = response.json()
        global current_access_token, token_expiry_time

        current_access_token = token_data.get("access_token")
        
        # Define o tempo de expiração para 'agora' + 'duração' - 'margem de segurança (60s)'
        expires_in = token_data.get("expires_in", 3600)
        token_expiry_time = time.time() + expires_in - 60 

        if current_access_token:
            print(f"✅ Novo token obtido com sucesso. Expira em {round(expires_in/60, 1)} minutos.")
            return current_access_token
        else:
            print("❌ Falha: 'access_token' não encontrado na resposta.")
            print(f"Resposta da API: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter o token Sankhya: {e}")
        return None

def get_current_token():
    """
    Verifica se o token atual é válido. Se estiver expirado (ou perto disso), solicita um novo.
    Isso garante o 'refresh' automático do token.
    """
    global current_access_token, token_expiry_time
    
    # Se o token não existir OU o tempo atual for maior ou igual ao tempo de expiração
    if current_access_token is None or time.time() >= token_expiry_time:
        return get_new_token()
    
    return current_access_token

# -------------------------------------------------------
# Fluxo Principal do Script
# -------------------------------------------------------
if __name__ == "__main__":
    
    # 1. AUTENTICAÇÃO COM A API SANKHYA
    print("--- 🔑 Autenticação da API Sankhya ---")
    access_token = get_current_token()
    
    if not access_token:
        # Se a autenticação da API for um requisito para o resto do script:
        print("Script encerrado. Falha na autenticação da API Sankhya.")
        exit() 
        
    print("-" * 40)

    # 2. CONEXÃO COM SQL SERVER
    try:
        print("--- 💾 Conexão com SQL Server ---")
        connection_string = (
            f"mssql+pyodbc://{SQLSERVER_USER}:{SQLSERVER_PASS}"
            f"@{SQLSERVER_HOST}:{SQLSERVER_PORT}/{SQLSERVER_DB}"
            f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
        )

        print("Conectando ao SQL Server...")
        engine = create_engine(connection_string, fast_executemany=True)
        conn = engine.connect()
        print("✅ Conectado ao SQL Server!")

    except Exception as e:
        print(f"❌ Erro na conexão com o SQL Server: {e}")
        exit()


    # 3. QUERY COM FILTRO
    query = text("""
        SELECT  
            cfp.CodigoContrato,
            cfp.Item,
            cfp.CodigoFormaPagamento,
            fp.Descricao,
            cfp.Valor,
            c.InseridoEm,
            CASE
                WHEN cfp.CodigoFormaPagamento IN (294) THEN 2
                WHEN cfp.CodigoFormaPagamento IN (296, 392, 393, 379) THEN 34
                WHEN cfp.CodigoFormaPagamento IN (319, 354) THEN 51
                WHEN cfp.CodigoFormaPagamento IN (297, 298, 299, 307, 312, 313, 315, 369, 372, 394, 395, 396, 397, 325, 326, 342, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 386) THEN 52
                WHEN cfp.CodigoFormaPagamento IN (2, 3, 4, 6, 8, 11, 12, 16, 21, 22, 29, 31, 32, 36, 37, 38, 39, 41, 43, 45, 48, 318, 327, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 355, 356, 357, 358, 359, 360, 361, 362, 364, 365, 366, 371, 376, 377, 381, 387, 388, 322, 328, 329, 370, 323, 324, 380, 389, 390) THEN 54
                WHEN cfp.CodigoFormaPagamento IN (290) THEN 58
                WHEN cfp.CodigoFormaPagamento IN (289) THEN 59
                WHEN cfp.CodigoFormaPagamento IN (288) THEN 60
                WHEN cfp.CodigoFormaPagamento IN (209) THEN 118
                WHEN cfp.CodigoFormaPagamento IN (210) THEN 119
                WHEN cfp.CodigoFormaPagamento IN (212) THEN 120
                WHEN cfp.CodigoFormaPagamento IN (215) THEN 121
                WHEN cfp.CodigoFormaPagamento IN (219) THEN 122
                WHEN cfp.CodigoFormaPagamento IN (224, 230, 237, 245, 254) THEN 123
                WHEN cfp.CodigoFormaPagamento IN (131) THEN 124
                WHEN cfp.CodigoFormaPagamento IN (132) THEN 125
                WHEN cfp.CodigoFormaPagamento IN (134) THEN 126
                WHEN cfp.CodigoFormaPagamento IN (137) THEN 127
                WHEN cfp.CodigoFormaPagamento IN (141) THEN 128
                WHEN cfp.CodigoFormaPagamento IN (146, 152, 159, 167, 176) THEN 129
                WHEN cfp.CodigoFormaPagamento IN (375) THEN 130
                WHEN cfp.CodigoFormaPagamento IN (385) THEN 132
                WHEN cfp.CodigoFormaPagamento IN (53) THEN 136
                WHEN cfp.CodigoFormaPagamento IN (54) THEN 137
                WHEN cfp.CodigoFormaPagamento IN (56) THEN 138
                WHEN cfp.CodigoFormaPagamento IN (59) THEN 139
                WHEN cfp.CodigoFormaPagamento IN (63) THEN 140
                WHEN cfp.CodigoFormaPagamento IN (68, 74, 81, 89, 98) THEN 141
                WHEN cfp.CodigoFormaPagamento IN (1) THEN 153
                WHEN cfp.CodigoFormaPagamento IN (320) THEN 165
                WHEN cfp.CodigoFormaPagamento IN (373) THEN 166
                ELSE cfp.CodigoFormaPagamento
            END AS CodigoPagamentoSankhya,
            CASE
                WHEN fp.Descricao LIKE '%%1X%%' THEN 1
                WHEN fp.Descricao LIKE '%%2X%%' THEN 2
                WHEN fp.Descricao LIKE '%%3X%%' THEN 3
                WHEN fp.Descricao LIKE '%%4X%%' THEN 4
                WHEN fp.Descricao LIKE '%%5X%%' THEN 5
                WHEN fp.Descricao LIKE '%%6X%%' THEN 6
                ELSE 1
            END AS QuantidadeParcelas,
            CASE
                WHEN fp.Descricao LIKE '%%Boleto%%' THEN 30
                WHEN fp.Descricao LIKE '%%Faturamento%%' THEN 30
                WHEN fp.Descricao LIKE '%%Crédito%%' THEN 30
                WHEN fp.Descricao LIKE '%%Débito%%' THEN 1
                WHEN fp.Descricao LIKE '%%Rentcars%%' THEN 30
                WHEN fp.Descricao LIKE '%%American%%' THEN 30
                ELSE 0
            END AS Prazo
        FROM ContratosFormaPagamento cfp
        LEFT JOIN FormaPagamento fp ON fp.CodigoFormaPagamento = cfp.CodigoFormaPagamento
        LEFT JOIN Contratos c ON c.CodigoContrato = cfp.CodigoContrato
        WHERE CAST(c.InseridoEm AS date) = CAST(DATEADD(DAY, -20, GETDATE()) AS date)
        ORDER BY cfp.CodigoContrato ASC
    """)

    # 4. LER EM CHUNKS
    print("\n--- 🔍 Lendo dados do SQL Server (em chunks) ---")
    chunk_size = 5000
    lista_contratos = []

    try:
        # Usando execution_options(stream_results=True) para leitura eficiente
        result = conn.execution_options(stream_results=True).execute(query)

        while True:
            rows = result.fetchmany(chunk_size)
            if not rows:
                break
            # Converte as linhas (rows) para dicionários (mapping) e adiciona à lista
            lista_contratos.extend([row._mapping for row in rows])
            print(f"Carregados até agora: {len(lista_contratos)} linhas")

        print("✅ Leitura finalizada!")
        print(f"Total final: {len(lista_contratos)} registros")
        
        # Fecha a conexão do banco de dados
        conn.close() 
        engine.dispose()
        print("Conexão SQL Server fechada.")

        # 5. CONVERTER PARA DATAFRAME
        df = pd.DataFrame(lista_contratos)
        print("\n--- 📊 DataFrame Gerado ---")
        print(df)
        print(f"Formato do DataFrame: {df.shape}")
        
        # 6. Exemplo de uso do access_token (Se for necessário enviar os dados para a Sankhya)
        if access_token:
            print("\nO 'access_token' está disponível para chamadas subsequentes à API Sankhya.")
            # Aqui você faria a chamada para enviar o DataFrame para a API
            # Ex: make_sankhya_api_request("endpoint/importacao", method="POST", data=df.to_dict('records'))

    except Exception as e:
        print(f"❌ Erro durante a execução da query ou processamento: {e}")