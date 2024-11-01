from dotenv import load_dotenv
import paramiko
import os
from DB import DatabaseManager
from query import QUERY

load_dotenv()

SERVER_ADDRESS = os.getenv('SERVER_NAME')
DATABASENAME = os.getenv('DATABASE')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

sftp_server   = os.getenv('FTP_SERVER')
sftp_username  = "autotagginggroupsftp.rockymountpdncsftp"
sftp_password = "m8p7O9Zl8yf7mp9kM9r1OJFLmEjdLxQF"
sftp_port = 22

csv_file_path = "data.csv"
remote_path  = 'data.csv'

try:
    db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
    df = db_manager.fetch_data(db_manager.engine, QUERY)
    # print(df.head(2)) 

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"Connecting to {sftp_server} with username {sftp_username}")
    ssh.connect(hostname=sftp_server, username=sftp_username, password=sftp_password, port=sftp_port)

    sftp = ssh.open_sftp()
    sftp.put(csv_file_path, remote_path)
    sftp.close()

    print(f"Auto tag file transferred successfully with {len(df)} rows.")
    
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    ssh.close()  # Ensure SSH connection is closed

 
