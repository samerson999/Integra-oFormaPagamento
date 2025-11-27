import os
import requests
import json
import time
from dotenv import load_dotenv

# -------------------------------------------------------
# 1. CONFIGURAÇÃO E VARIÁVEIS DE AMBIENTE
# -------------------------------------------------------
load_dotenv("config/.env")

# Variáveis da API Sankhya (Gateway)
API_URL_AUTH = "https://api.sandbox.sankhya.com.br/authenticate"
# Endpoint de Teste
API_URL_DATA = "https://api.sandbox.sankhya.com.br/gateway/v1/mgecom/service.sbr?serviceName=CACSP.incluirAlterarFinanceiro&outputType=json"

API_CLIENT_ID = os.getenv("client_id")
API_CLIENT_SECRET = os.getenv("client_secret")
API_X_TOKEN = os.getenv("X-Token")

# Variáveis de controle do Token
current_access_token = None
token_expiry_time = 0 

# -------------------------------------------------------
# 2. FUNÇÕES DE AUTENTICAÇÃO E REFRESH DA API SANKHYA
# -------------------------------------------------------

def get_new_token():
    """Solicita um novo 'access_token' para a API Sankhya."""
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
        token_expiry_time = time.time() + expires_in - 60 
        return current_access_token
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter o token Sankhya: {e}")
        return None

def get_current_token():
    """Verifica e solicita um novo token se o atual estiver expirado."""
    global current_access_token, token_expiry_time
    if current_access_token is None or time.time() >= token_expiry_time:
        print("⏳ Solicitando novo token de acesso Sankhya...")
        token = get_new_token()
        if token:
             print("✅ Novo token obtido com sucesso.")
        return token
    return current_access_token

# -------------------------------------------------------
# 3. FUNÇÃO DE TESTE E ANÁLISE DO SERVIÇO
# -------------------------------------------------------

def run_test_service_call(access_token):
    """
    Executa a chamada de teste do CACSP.incluirAlterarFinanceiro com o payload fixo.
    """
    print("\n--- 📤 Iniciando Chamada de Teste ---")
    
    headers = {
        "Authorization": f"Bearer {access_token}", 
        "Content-Type": "application/json"
    }

    # PAYLOAD HARDCODED: Usamos o payload exato que você forneceu.
    # O valor de 'nufin': "219900" será a chave de ligação que o serviço tentará alterar/reprocessar.
    payload = {
        "serviceName": "CACSP.incluirAlterarFinanceiro",
        "requestBody": {
            "nota": {
                "nufin": "219919",
                "isParcelamentoVariosTipTit": "true",
                "itens": {
                    "item": [
                        {    
                            "DTVENC": {"$": "20/11/2025"},                    
                            "VLRDESDOB": {"$": "902.56"},
                            "CODTIPTIT": {"$": "166"},
                            "QTDPARCELAS": {"$": "1"},
                            "PRAZOPARCELAS": {"$": "30"}
                        },
                        {
                            "DTVENC": {"$": "20/11/2025"},
                            "VLRDESDOB": {"$": "240.69"},
                            "CODTIPTIT": {"$": "125"},
                            "QTDPARCELAS": {"$": "2"},
                            "PRAZOPARCELAS": {"$": "30"}
                        }
                    ]
                }
            }
        }
    }

    try:
        response = requests.post(API_URL_DATA, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Captura erros HTTP (4xx, 5xx)

        response_data = response.json()
        
        print("\n--- ✅ Resposta Completa da API Sankhya ---")
        print(json.dumps(response_data, indent=4))
        
        # Análise da Resposta Sankhya
        print("\n--- 🔍 Análise de Status ---")
        status = response_data.get("status")
        
        if status == "1":
            new_nufin_pk = response_data.get("responseBody", {}).get("pk", {}).get("NUFIN", {}).get("$", "N/A")
            print(f"SUCESSO: O serviço retornou STATUS 1. Novo/Atualizado NUFIN: {new_nufin_pk}")
            print("Isso confirma que o endpoint, autenticação e payload estão funcionalmente corretos.")
        else:
            error_msg = response_data.get("statusMessage", "Mensagem de erro não especificada.")
            print(f"FALHA: O serviço retornou STATUS 0 ou outro código.")
            print(f"Mensagem do Sistema: {error_msg}")
            
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ ERRO HTTP: Falha na requisição. Código: {e.response.status_code}")
        print(f"Mensagem: {e}")
        try:
            print(f"Resposta bruta da API: {e.response.text}")
        except:
            pass
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERRO DE CONEXÃO: {e}")
    except Exception as e:
        print(f"\n❌ ERRO INESPERADO: {e}")


# -------------------------------------------------------
# 4. FLUXO PRINCIPAL
# -------------------------------------------------------
if __name__ == "__main__":
    
    # A. AUTENTICAÇÃO
    print("--- 🔑 Teste de Autenticação da API Sankhya ---")
    access_token = get_current_token()
    
    if not access_token:
        print("\nScript encerrado. Falha na autenticação da API Sankhya.")
        exit() 
        
    # B. CHAMADA DO SERVIÇO DE TESTE
    run_test_service_call(access_token)