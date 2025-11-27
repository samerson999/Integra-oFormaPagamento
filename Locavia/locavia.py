import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------
# Carregar variáveis de ambiente
# -------------------------------------------------------
load_dotenv("config/.env")

# Variáveis do SQL Server Locavia
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST2")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT")
SQLSERVER_DB = os.getenv("SQLSERVER_DB")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS")
driver = "ODBC Driver 18 for SQL Server"

def get_db_engine():
    """Cria e retorna o objeto Engine do SQLAlchemy para o SQL Server Locavia."""
    try:
        connection_string = (
            f"mssql+pyodbc://{SQLSERVER_USER}:{SQLSERVER_PASS}"
            f"@{SQLSERVER_HOST}:{SQLSERVER_PORT}/{SQLSERVER_DB}"
            f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string, fast_executemany=True)
        return engine
    except Exception as e:
        print(f"❌ Erro na configuração do Engine/Conexão com o SQL Server Locavia: {e}")
        return None

def get_contratos_locavia():
    """
    Executa a query, carrega os dados do Locavia, agrupa por CodigoContrato 
    e retorna uma lista de contratos prontos para o Sankhya.
    """
    engine = get_db_engine()
    if not engine:
        return []

    # Sua query original do Locavia
    query = text("""
        SELECT  
            cfp.CodigoContrato,
            cfp.Item,
            cfp.Valor,
            c.InseridoEm, -- Mantido para fins de referência, mas não será usado para DTVENC
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
        WHERE c.CodigoContrato in (79297, 79299, 79300) 
        ORDER BY cfp.CodigoContrato ASC
    """)

    try:
        print("\n--- 🔍 Lendo dados do SQL Server Locavia ---")
        df = pd.read_sql(query, engine)
        print(f"✅ Leitura finalizada! Total: {len(df)} registros.")

        # Agrupa os dados por CodigoContrato
        contratos_agrupados = []
        
        # Agrupa pelo CodigoContrato e itera sobre cada grupo (contrato)
        for cod_contrato, grupo in df.groupby('CodigoContrato'):
            
            # Formata os itens de pagamento para o payload Sankhya
            itens_pagamento = []
            for index, row in grupo.iterrows():
                item = {
                    "DTVENC": {"$": ""},  # será preenchido no main
                    "VLRDESDOB": {"$": f"{row['Valor']:.2f}"},
                    "CODTIPTIT": {"$": str(row['CodigoPagamentoSankhya'])},
                    "QTDPARCELAS": {"$": str(row['QuantidadeParcelas'])},
                    "PRAZOPARCELAS": {"$": str(row['Prazo'])}
                    }

                itens_pagamento.append(item)
            
            # Adiciona o contrato agrupado na lista final
            contratos_agrupados.append({
                "CodigoContrato": str(cod_contrato),
                "ItensPagamento": itens_pagamento
            })

        print(f"Total de Contratos Únicos para processar: {len(contratos_agrupados)}")
        return contratos_agrupados

    except Exception as e:
        print(f"❌ Erro durante a execução da query ou processamento de dados Locavia: {e}")
        return []

if __name__ == "__main__":
    # Exemplo de como usar a função
    import time
    contratos = get_contratos_locavia()
    if contratos:
        print("\n--- Exemplo de Contrato Agrupado (Primeiro Registro) ---")
        print(json.dumps(contratos[0], indent=4))