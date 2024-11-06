import pandas as pd
import requests
from dotenv import load_dotenv
import os
from DB import DatabaseManager

# load_dotenv()

# SERVER_ADDRESS = os.getenv('SERVER_NAME')
# DATABASENAME = os.getenv('DATABASE')
# DB_USERNAME = os.getenv('DB_USERNAME')
# DB_PASSWORD = os.getenv('DB_PASSWORD')

# API_KEY = os.getenv("GOOGLE_API_KEY")

# dm = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)


# df = pd.read_csv('data.csv')[:3]

# lat_lon = []

# for index, row in df.iterrows():
#     geox=row['geox']
#     geoy=row['geoy']
#     if geox == 0 or geoy == 0:
#         lat_lon.append((35.94714, -77.79662))
#     else:
#         latlong = dm.get_cords(geox, geoy)
#         lat_lon.append(latlong)
#         print(f"{index} - {latlong}")
    
# df['lat_lon'] = lat_lon

# df.to_csv('data2.csv', index=False)


def get_zipcode(lat, lon):
    api_key = os.getenv("GOOGLE_API_KEY")
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        for result in data["results"]:
            for component in result["address_components"]:
                if "postal_code" in component["types"]:
                    zip_code = component["long_name"]
                    print(zip_code)
                    return zip_code
    else:
        print("Error:", response.status_code)
        return None
        

df = pd.read_csv('data2.csv')  

zip_codes = []

for lat_lon in df['lat_lon']:
    lat_lon = eval(lat_lon)
    zip_code = get_zipcode(lat_lon[0], lat_lon[1])
    zip_codes.append(zip_code) 
    
df['Zip Code'] = zip_codes

df.to_csv('data_zipcodes.csv', index=False, 
          columns=[
                'Event ID', 
                'Report Number', 
                'Officer Badge ID', 
                'Officer Dispatched DateTime', 
                'Officer Cleared DateTime', 
                'Street', 
                'City', 
                'State', 
                'Zip Code', 
                'Call Type', 
                'Clearance Code', 
                'Category', 
                'Category 2'
            ])