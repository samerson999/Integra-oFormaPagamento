import requests

url = "https://api.sandbox.sankhya.com.br/authenticate"

payload = {
    "grant_type": "client_credentials",
    "client_id": "",
    "client_secret": ""
}
headers = {
    "accept": "application/x-www-form-urlencoded",
    "X-Token": "",
    "content-type": "application/x-www-form-urlencoded"
}

response = requests.post(url, data=payload, headers=headers)

print(response.text)


