import requests

def fetch_data(endpoint):
    response = requests.get(endpoint, timeout=30)
    response.raise_for_status()
    return response.json()