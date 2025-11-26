import requests

url = "https://api.sandbox.sankhya.com.br/authenticate"

payload = {
    "grant_type": "client_credentials",
    "client_id": "b6b8083b-b0fe-4dab-9f3a-62885f7c50f6",
    "client_secret": "CHMh52Gb0ZKH1HbbzRUP2rJ2ALztm27W"
}
headers = {
    "accept": "application/x-www-form-urlencoded",
    "X-Token": "64a49033-27db-46b5-93f2-b9fb37c3fed8",
    "content-type": "application/x-www-form-urlencoded"
}

response = requests.post(url, data=payload, headers=headers)

print(response.text)


