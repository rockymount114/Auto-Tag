from dotenv import load_dotenv
import paramiko
import os, time, socket
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

# def to_sftp():
#     sftp_server   = os.getenv('FTP_SERVER')
#     sftp_username  = os.getenv('FTP_USERNAME')
#     sftp_password =  os.getenv('FTP_PASSWORD')
#     sftp_port = os.getenv('FTP_PORT')

#     # csv_file_path = "data_gis.csv"
#     csv_file_path = "data.csv" # change to use data.csv directly
#     remote_path  = 'data.csv'
#     ssh=None

#     try:
#         df=pd.read_csv(csv_file_path)
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         ssh.connect(hostname=sftp_server, username=sftp_username, password=sftp_password, port=sftp_port)

#         sftp = ssh.open_sftp()
#         sftp.put(csv_file_path, remote_path)
#         sftp.close()

#         print(f"Auto tag file transferred successfully with {len(df)} rows.")
#         with open('log.txt', 'a') as f:
#             current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             f.write(f"{current_time}: Auto tag file transferred successfully with {len(df)} rows.\n")
        
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     finally:
#         if ssh:
#             ssh.close()

def to_sftp():
    sftp_server = os.getenv('FTP_SERVER')
    sftp_username = os.getenv('FTP_USERNAME')
    sftp_password = os.getenv('FTP_PASSWORD')
    sftp_port = os.getenv('FTP_PORT', '22')  # Keep as string initially
    
    # Validate environment variables
    if not sftp_server:
        print("ERROR: FTP_SERVER environment variable is not set")
        return False
    
    if not sftp_username:
        print("ERROR: FTP_USERNAME environment variable is not set")
        return False
    
    if not sftp_password:
        print("ERROR: FTP_PASSWORD environment variable is not set")
        return False
    
    try:
        sftp_port = int(sftp_port)
    except ValueError:
        print(f"ERROR: Invalid FTP_PORT value: {os.getenv('FTP_PORT')}, using default 22")
        sftp_port = 22

    csv_file_path = "data.csv"
    remote_path = 'data.csv'
    ssh = None
    sftp = None
    
    # Connection parameters
    max_retries = 3
    retry_delay = 5
    timeout = 30

    try:
        # Read CSV first to get row count
        df = pd.read_csv(csv_file_path)
        
        # Retry logic for connection
        for attempt in range(max_retries):
            try:
                print(f"Attempting SFTP connection to {sftp_server}:{sftp_port} (attempt {attempt + 1}/{max_retries})...")
                
                # Create SSH client with enhanced settings
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # Connect with timeout and additional parameters
                ssh.connect(
                    hostname=sftp_server,
                    username=sftp_username,
                    password=sftp_password,
                    port=sftp_port,
                    timeout=timeout,
                    auth_timeout=timeout,
                    banner_timeout=timeout,
                    compress=True,
                    look_for_keys=False,  # Don't look for SSH keys
                    allow_agent=False,    # Don't use SSH agent
                    sock=None
                )
                
                # Set keep-alive to prevent connection drops
                ssh.get_transport().set_keepalive(30)
                
                # Open SFTP session
                sftp = ssh.open_sftp()
                
                # Upload file
                print(f"Uploading {csv_file_path} to {remote_path}...")
                sftp.put(csv_file_path, remote_path)
                
                # Success - break out of retry loop
                print(f"Auto tag file transferred successfully with {len(df)} rows.")
                with open('log.txt', 'a') as f:
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{current_time}: Auto tag file transferred successfully with {len(df)} rows.\n")
                
                return True  # Exit retry loop on success
                
            except (paramiko.AuthenticationException, paramiko.SSHException, socket.error, OSError) as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                
                # Close connections if they exist
                if sftp:
                    try:
                        sftp.close()
                    except:
                        pass
                    sftp = None
                
                if ssh:
                    try:
                        ssh.close()
                    except:
                        pass
                    ssh = None
                
                # If this was the last attempt, raise the exception
                if attempt == max_retries - 1:
                    raise
                
                # Wait before retrying
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
    except Exception as e:
        error_msg = f"An error occurred: {e}"
        print(error_msg)
        with open('log.txt', 'a') as f:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{current_time}: {error_msg}\n")
        return False
        
    finally:
        # Ensure proper cleanup
        if sftp:
            try:
                sftp.close()
            except:
                pass
        
        if ssh:
            try:
                ssh.close()
            except:
                pass
            

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
    
    
    # get_all_zipcode(df=df)    
    
    to_sftp()
    
    send_email_notification(df[:3])

    
    
    
    