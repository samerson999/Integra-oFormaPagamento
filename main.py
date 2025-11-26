import requests
import json
import pandas as pd

url = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json"

headers = {
    "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICItVmZ0UXFjUmhnS0JkNmpLdE5QM3B4a3dsNjJsR2lPLXJMZlNCTjFldWhFIn0.eyJleHAiOjE3NjQxODkzNTMsImlhdCI6MTc2NDE4OTA1MywianRpIjoidHJydGNjOmEwZjY1OGUwLWYwZjctNDRmNi1hYjMzLTE4NjRhZjJjMjgyNSIsImlzcyI6Imh0dHBzOi8vYXBpLnNhbmRib3guc2Fua2h5YS5jb20uYnIvcmVhbG1zL2ludGVncmFjb2VzIiwic3ViIjoiZWZlZmE2M2YtMjdmNi00YjdhLWI4NDYtNWFlNzdiNDUwYzYxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYjZiODA4M2ItYjBmZS00ZGFiLTlmM2EtNjI4ODVmN2M1MGY2IiwiY29kUGFyYyI6IlI1RjcrU0trRlNveDhpc2oyL3l6K2c9PSIsImNsaWVudEhvc3QiOiIxMC40Mi4xLjAiLCJqc2Vzc2lvbklkIjoiV0pIeTFjbDNOVElvZzFHNk9xT0w5Q0VqQ05LWUNwdUVKblFXUTlLL1ZQSlpBbk50cHYrRGdoR040Q1VDUVY4QiIsImFtYmllbnRlIjoiaG1sIiwibm9tZUludGVncmFkb3IiOiJURHRHMjVQeGhtR2o2bGJFK3FVMzU5NlNCN2NTaUVxelpPcXdjMXl1dGN3R3cycWpiWWswejBpcGt5TktQZ0NIIiwiY2xpZW50QWRkcmVzcyI6IjEwLjQyLjEuMCIsImNsaWVudF9pZCI6ImI2YjgwODNiLWIwZmUtNGRhYi05ZjNhLTYyODg1ZjdjNTBmNiIsInVybCI6IjJLRG9OUmVZZzBBSUE4cysyV1M4MXViakZPbVJabm1lUzNSeFJwczFqZWMzZG4zU2RYWnZ0eUlwMlMrRitxa2oiLCJvcmlnZW1BcGxpY2FjYW8iOiJseXFDZDZ5YVJUMGVsYkxRem1sRXl3PT0iLCJwbGFpbk5vbWVBcGxpY2FjYW8iOiJEZXRhbGhhbWVudG8gZmluYW5jZWlybyIsIm5vbWVBcGxpY2FjYW8iOiJOdHdHNERvVEhWSjdhWE9YRFlLY1ZSb2QrVGdwTUZkWnYyMHVaS0VmNEk0PSIsInBsYWluTm9tZUludGVncmFkb3IiOiJBU0EgUkVOVCBBIENBUiBMT0NBQ0FPIERFIFZFSUNVTE9TIExUREEiLCJzY29wZSI6WyJlcnA6YWxsIl0sIm5vbWVDbGllbnRlIjoiVER0RzI1UHhobUdqNmxiRStxVTM1OTZTQjdjU2lFcXpaT3F3YzF5dXRjd0d3MnFqYllrMHowaXBreU5LUGdDSCIsImFwcGxpY2F0aW9uSWQiOiJld3A2ZnFVZGRnd0tGWjY1YjdlaFNvSVFEY0NUcWVHS1VvMXZyL1dkY2VuSHIzVExuVWRaRGVGRzJlby95V1plIn0.FKKOTk1wEULiBw_2HVA53OiQ2uNV22S08KYrNlmNHGTDVAWDDifLyxVsZIi6JOkja7tf8JYhHLa5ef-vRThaicz5TnZHjK-fPgmfNzSbj5YQ_h-VV58XlMpYGoBneMnmkBALU5DMMBgwpW00VHdO0Bhc4h5RIo7RpwtWNhtwlItHzZwM3tBF3s6XiZ0q1nadEtI4a3d8dAnxqtdNe1pPfPcBE96x_vbXJDI7XcmRrJmVI3ps285x2TompOadRYVl2pGHi6bzQh8khI3ZHFYWlzFFkoKCjbfrhifckU0lVomRAdURz52Jbz_G9rZFkULPZHOnhWB1eR5UcChJWC9C5OsY_1yIlrKed1dGdePXwhGrylLxLWdHJQQ9uG0XJkw4MifKzUkqkk4gFpIX538ny4Yf9n9X2ZfoHWm4D5Q14JmpuToVrDR6-cT4_cyWOUlYvhUlOZdZVzHzHmaTCYXlOuy7x43r1C_d9CfwiYTndNPD2r4YQhmhxu2eI2hI8PlSxmX3AzZ1q29Ev7qu5HvwsMUE2ySfBkZSx9bkj47dZN2qAhxeqzUxcEHlXm2CCZawey2VdRGULUlqhq3pTG33iR5I0w-QvlybsG3PlgoiJlKm-4J9b4G6KKcVyhxuJ6euU_XzXrCWnsJS3obogh5O_km1JiKxFlltW_QBzc7xpzM",
    "Content-Type": "application/json"
}

# ==============================
# 2. JSON EXATO (exemplo simples)
# ==============================
payload = {
    "serviceName": "CRUDServiceProvider.loadRecords",
    "requestBody": {
        "dataSet": {
            "rootEntity": "CabecalhoNota",
            "ignoreCalculatedFields": "true",
            "useFileBasedPagination": "true",
            "includePresentationFields": "N",
            "tryJoinedFields": "true",
            "offsetPage": "0",
            "criteria": {
                "expression": {
                    "$": "1 = 1"
                }
            },
            "entity": [
                {
                    "path": "",
                    "fieldset": {
                        "list": "NUMNOTA, NUNOTA"
                    }
                }
            ]
        }
    }
}

# ==============================
# 3. Requisição
# ==============================
response = requests.post(url, headers=headers, data=json.dumps(payload))
dados = response.json()

# ==============================
# 4. Extração dos registros
# ==============================
try:
    registros = dados["responseBody"]["entities"]["entity"]
except:
    print("⚠ Não encontrei a chave esperada no retorno Sankhya:")
    print(json.dumps(dados, indent=4))
    registros = []

# ==============================
# 5. Converter para DataFrame
# ==============================
df = pd.DataFrame(registros)

print(df)
