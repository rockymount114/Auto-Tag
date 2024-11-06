'''
1. Google API Pricing: 2,500 requests per day for free; then $0.005 per request.
2. USPS API free but need business account to get API key.
3. Geocodio API Pricing: 2,500 requests per day for free; then $0.005 per request, if unlimited starts at $1,000/month, 11,400/year.
'''


import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("GOOGLE_API_KEY")



def get_zip_code(address, city, state):
    """Pricing: 2,500 requests per day for free; then $0.005 per request."""
    
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": f"{address}, {city} ,{state}",
        "key": os.getenv("GOOGLE_API_KEY")
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if data["status"] == "OK":
        for component in data["results"][0]["address_components"]:
            if "postal_code" in component["types"]:
                zipcode = component["long_name"]
                return zipcode
    return None
    


df = pd.read_csv('data.csv')

zip_codes = []

for index, row in df.iterrows():  
    if pd.isna(row['Zip Code']):
        address = row['Street']
        city = row['City']
        state = row['State']
        zip_code = get_zip_code(address, city, state)
        zip_codes.append(zip_code)     
        print(f"Processed row {index + 1} of {len(df)}, Zip Code: {zip_code}")
    else:
        zip_codes.append(row['Zip Code'])

# Replace the 'Zip Code' column in df with the new zip codes
df['Zip Code'] = zip_codes

# Save the updated DataFrame to a new CSV file
df.to_csv('new_data_with_zip_code.csv', index=False)