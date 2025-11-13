import requests
import pandas as pd
import time
from datetime import datetime

from tokens import (
    api_key_arguibel,
    api_secret_arguibel,
    api_key_polo,
    api_secret_polo,
    api_key_ugarte,
    api_secret_ugarte,
)

def autenticar(api_key: str, api_secret: str, max_retries: int = 3) -> dict:
    auth_url = 'https://auth.fu.do/api'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'}
    json_data = {
        'apiKey': api_key,
        'apiSecret': api_secret,
    }
    
    for attempt in range(max_retries + 1):
        response = requests.post(auth_url, headers=headers, json=json_data)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))  # Default to 60 seconds
            print(f"Authentication rate limited. Waiting {retry_after} seconds (attempt {attempt + 1}/{max_retries + 1})")
            if attempt < max_retries:
                time.sleep(retry_after)
                continue
            else:
                raise Exception(f"Authentication failed after {max_retries + 1} attempts due to rate limiting")
        
        # Handle other non-200 responses
        if response.status_code != 200:
            if attempt < max_retries:
                print(f"Authentication failed with status {response.status_code}, retrying in 5 seconds (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(5)
                continue
            else:
                raise Exception(f"Authentication failed after {max_retries + 1} attempts: {response.text}")
        
        # Success case
        response_data = response.json()
        if response_data.get('token'):
            return {
                'accept': 'application/json',
                'authorization': f'Bearer {response_data["token"]}',
            }
        else:
            if attempt < max_retries:
                print(f"No token received, retrying (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(5)
                continue
            else:
                raise Exception(f"No token received after {max_retries + 1} attempts")
    
    # Should never reach here, but just in case
    raise Exception("Authentication failed unexpectedly")

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

