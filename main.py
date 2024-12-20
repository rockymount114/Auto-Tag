from dotenv import load_dotenv
import paramiko
import os
from datetime import datetime
from DB import DatabaseManager, EmailManager, EmailMessage
from query import QUERY
import pandas as pd




######### Get zip code using GIS API
def get_all_zipcode(df):
    """Get zip code using GIS API.

    Args:
        df (pd.DataFrame): pandas dataframe containing address information.
    """
    zip_code_map = {
        'GATEWAY HOTELS': '27804',
        'WAWA': '27804',
        'ROCKY MOUNT HIGH': '27803',
        'Unit 970 chasing': '27803',
        '8197 EDWARDS RD': '27816',
        'DOWNTOWN':'27802',
        'ROCKY MOUNT CHRYSLER DODGE JEEP RAM':'27803',
        'NASH COUNTY JAIL':'27856',
        'I95N/US64W':'27804',
        'WATSON SEED FARM RD':'27891'
    }
    
    def fetch_zip_code(row):
        street = row['Street']        
        if street in zip_code_map:
            return zip_code_map[street]
        
        address = f"{street}, {row['City']}, {row['State']}"
        zip_code = dm.address_to_zip(address)
        if zip_code is None:
            # print(f"Warning: No zip code found for address: {address}") 
            return ''
        return zip_code

    df['Zip Code'] = df.apply(fetch_zip_code, axis=1)

    df.to_csv('data_gis.csv', index=False, columns=[
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
        'Custom Address'
    ])
    


######### send data to Axon

def to_sftp():
    sftp_server   = os.getenv('FTP_SERVER')
    sftp_username  = os.getenv('FTP_USERNAME')
    sftp_password =  os.getenv('FTP_PASSWORD')
    sftp_port = os.getenv('FTP_PORT')

    csv_file_path = "data_gis.csv"
    remote_path  = 'data.csv'
    ssh=None

    try:
        df=pd.read_csv(csv_file_path)
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
        if ssh:
            ssh.close()

def send_email_notification(df):
    
    load_dotenv()
    
    email_manager = EmailManager(os.getenv('EMAIL_ADDRESS'), os.getenv('EMAIL_PASSWORD'))
    subject = "Auto Tag"
    body = f"Auto Tag file transferred successfully.\n\n{df.head()}"
    recipient = os.getenv('RECIPIENT')
    email_manager.send_email(subject, body, recipient)
 

if __name__ == "__main__":
    
    load_dotenv()

    SERVER_ADDRESS = os.getenv('SERVER_NAME')
    DATABASENAME = os.getenv('DATABASE')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    dm = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    df = dm.fetch_data(dm.engine, QUERY)
    
    
    get_all_zipcode(df=df)    
    
    to_sftp()
    
    # send_email_notification(df[:3])

    
    
    
    