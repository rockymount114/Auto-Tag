"""get zipcode using USPS API"""

import pandas as pd
import requests
import random
import time

df = pd.read_csv('data.csv')


def get_zip(addr: str, city: str, state: str):
        """Get zip code for those 00000 record"""
        
        url = "https://tools.usps.com/tools/app/ziplookup/zipByAddress"

        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://tools.usps.com',
            'referer': 'https://tools.usps.com/zip-code-lookup.htm?byaddress',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
        }

        payload = {
            'address1': addr,
            'city': city,
            'state': state
        }
        resp = requests.post(url=url, data=payload, headers=headers)
        time.sleep(random.randint(1, 2))
        # Parse the JSON response
        response_json = resp.json()

        if response_json['resultStatus'] == "SUCCESS" and response_json['addressList']:
            return response_json['addressList'][0]['zip5']
        else:
            return None
        
# Initialize a list to store zip codes
zip_codes = []

for index, row in df.iterrows():    
    address = row['Street']
    city = row['City']
    state = row['State']
    zip_code = get_zip(address, city, state)
    zip_codes.append(zip_code) 
    time.sleep(random.randint(1, 2))
    print(f"Processed row {index + 1} of {len(df)}, Zip Code: {zip_code}")

# Replace the 'Zip Code' column in df with the new zip codes
df['Zip Code'] = zip_codes

# Save the updated DataFrame to a new CSV file
df.to_csv('new_data_with_zip_code.csv', index=False)