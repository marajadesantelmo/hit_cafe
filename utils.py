import requests
import pandas as pd
from datetime import datetime

from tokens import (
    api_key_arguibel,
    api_secret_arguibel,
    api_key_polo,
    api_secret_polo,
    api_key_ugarte,
    api_secret_ugarte,
)

def autenticar(api_key: str, api_secret: str) -> dict:
    auth_url = 'https://auth.fu.do/api'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'}
    json_data = {
    'apiKey': api_key,
    'apiSecret': api_secret,}
    response = requests.post(auth_url, headers=headers, json=json_data)
    if response.status_code != 200:
        raise Exception(f"Authentication failed: {response.text}")
    token = response.json().get('token')

    if not token:
        raise Exception("No token received in the response.")
    return {
        'accept': 'application/json',
        'authorization': f'Bearer {token}',
    }

def get_sale_data(headers, sale_id):
    url = f'https://api.fu.do/v1alpha1/sales/{sale_id}?include=items'
    response = requests.get(url, headers=headers)
    response = response.json()
    response = response['data']
    return response


def get_branch_configs():
    """Return the API credentials for each sucursal.

    Output example:
    [
        {"name": "Arguibel", "apiKey": "...", "apiSecret": "..."},
        ...
    ]
    """
    return [
        {"name": "Arguibel", "apiKey": api_key_arguibel, "apiSecret": api_secret_arguibel},
        {"name": "Polo", "apiKey": api_key_polo, "apiSecret": api_secret_polo},
       # {"name": "Ugarte", "apiKey": api_key_ugarte, "apiSecret": api_secret_ugarte},
    ]

