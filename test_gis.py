import pandas as pd
import requests
from dotenv import load_dotenv
import os
from DB import DatabaseManager

load_dotenv()

def address_to_zip(address):
    url = "https://geocode-api.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
    params = {
        "f": "pjson",
        "singleLine": address,
        "token": os.getenv("GIS_TOKEN")
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data=response.json()
        address = data['candidates'][0]['address']
        zipcode = address.split(',')[-1].strip()
        if len(zipcode) == 5 and zipcode.isdigit():
            return zipcode
        else:
            return None
    else:
        # print("Error:", response.status_code)
        with open('error_log.txt', 'a') as f:
            f.write(f"Error processing address: {address}\n")
        return None
        
        

df = pd.read_csv('data.csv')

zip_codes = []

for index, row in df.iterrows():
    if (row['Street']=='GATEWAY HOTELS') | (row['Street']=='WAWA'):
        zip_codes.append('27804')
        continue
    if (row['Street']=='ROCKY MOUNT HIGH') | (row['Street']=='Unit 970 chasing'):
        zip_codes.append('27803')
        continue   
    address = row['Street'] + ', ' + row['City'] + ', ' + row['State']
    zip_code = address_to_zip(address)
    zip_codes.append(zip_code) 
    
    print(f"Processed row {index + 1} of {len(df)}, Zip Code: {zip_code}")


df['Zip Code'] = zip_codes

df.to_csv('data_gis.csv', index=False)