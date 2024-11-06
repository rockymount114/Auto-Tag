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
# print(df.head(3))


# sftp_server   = os.getenv('FTP_SERVER')
# sftp_username  = os.getenv('FTP_USERNAME')
# sftp_password =  os.getenv('FTP_PASSWORD')
# sftp_port = os.getenv('FTP_PORT')

# csv_file_path = "data.csv"
# remote_path  = 'data.csv'

# try:
#     db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
#     df = db_manager.fetch_data(db_manager.engine, QUERY)
#     # print(df.head(2)) 

#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(hostname=sftp_server, username=sftp_username, password=sftp_password, port=sftp_port)

#     sftp = ssh.open_sftp()
#     sftp.put(csv_file_path, remote_path)
#     sftp.close()

#     print(f"Auto tag file transferred successfully with {len(df)} rows.")
#     with open('log.txt', 'a') as f:
#         current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         f.write(f"{current_time}: Auto tag file transferred successfully with {len(df)} rows.\n")
    
# except Exception as e:
#     print(f"An error occurred: {e}")
# finally:
#     ssh.close()

 
