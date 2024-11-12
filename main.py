from dotenv import load_dotenv
import paramiko
import os
from datetime import datetime
from DB import DatabaseManager
from query import QUERY

load_dotenv()

SERVER_ADDRESS = os.getenv('SERVER_NAME')
DATABASENAME = os.getenv('DATABASE')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

dm = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
df = dm.fetch_data(dm.engine, QUERY)


######### Get zip code using GIS API

zip_codes = []

for index, row in df.iterrows():
    if (row['Street']=='GATEWAY HOTELS') | (row['Street']=='WAWA'):
        zip_codes.append('27804')
        continue
    if (row['Street']=='ROCKY MOUNT HIGH') | (row['Street']=='Unit 970 chasing'):
        zip_codes.append('27803')
        continue   
    address = row['Street'] + ', ' + row['City'] + ', ' + row['State']
    zip_code = dm.address_to_zip(address)
    zip_codes.append(zip_code) 
    
    print(f"Processed row {index + 1} of {len(df)}, Zip Code: {zip_code}")


df['Zip Code'] = zip_codes

df.to_csv('data_gis.csv', index=False,
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
                'Category'
            ])


######### send data to Axon

sftp_server   = os.getenv('FTP_SERVER')
sftp_username  = os.getenv('FTP_USERNAME')
sftp_password =  os.getenv('FTP_PASSWORD')
sftp_port = os.getenv('FTP_PORT')

csv_file_path = "data_gis.csv"
remote_path  = 'data.csv'

try:
    db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    df = db_manager.fetch_data(db_manager.engine, QUERY)
    # print(df.head(2)) 

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=sftp_server, username=sftp_username, password=sftp_password, port=sftp_port)

    sftp = ssh.open_sftp()
    sftp.put(csv_file_path, remote_path)
    sftp.close()

    print(f"Auto tag file transferred successfully with {len(df)} rows.")
    with open('log.txt', 'a') as f:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{current_time}: Auto tag file transferred successfully with {len(df)} rows.\n")
    
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    ssh.close()

 
