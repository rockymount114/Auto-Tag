import requests
import os

def get_token():
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    arcgis_url = "https://www.arcgis.com"  # or your ArcGIS Enterprise URL

    # Set the token endpoint URL
    token_url = f"{arcgis_url}/sharing/rest/oauth2/token"

    # Set the token request parameters
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    # Make the token request
    response = requests.post(token_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the token from the response
        token = response.json()["access_token"]    
        # print(f"Token: {token}")
        return token
    else:
        # print(f"Error: {response.status_code}")
        return None