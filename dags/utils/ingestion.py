import requests

def fetch_data(endpoint):
    try:
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        print(f"Request to {endpoint} timed out.")
        return None
    except requests.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
