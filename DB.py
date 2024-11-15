import os
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pyodbc
from dotenv import load_dotenv
import pyproj
import smtplib
import requests
from email.message import EmailMessage
from query import QUERY

class DatabaseManager:
    def __init__(self, server_address, database, username, password, port=1433):
        self.server_address = server_address
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.engine = self.create_db_engine()
        
        
        # self.email_address = os.getenv('EMAIL_ADDRESS')
        # self.email_password = os.getenv('EMAIL_PASSWORD')

    def create_db_engine(self):
        try:
            conn = URL.create(
                "mssql+pyodbc",
                username=self.username,
                password=self.password,
                host=self.server_address,
                database=self.database,
                query={"driver": "ODBC Driver 17 for SQL Server"}
            )
            engine = create_engine(conn)
            return engine
        
        except sqlalchemy.exc.OperationalError as e:
            print(f"Operational Error: {e}")
            return None
        except Exception as e:
            print(f"Other Error: {e}")
            return None
            
    def fetch_data(self, engine, query):
        '''fetch the data from database using SQL query'''
        if engine is None:
            print("Engine is not initialized.")
            return None
        try:
            query = text(query)
            df = pd.read_sql(query, engine)
            df.to_csv('data.csv', index=False, sep=',', quoting=1)
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None
        
    def write_to_csv(self, df, filename):
        """write a dataframe to a CSV file"""
        df.to_csv(filename, index=False, sep=',', quoting=1)
          
    # Using GIS API to get zip code
    
    @staticmethod
    def get_token():
        """get token from ArcGIS API"""
        
        load_dotenv()
        
        client_id = os.getenv("GIS_CLIENT_ID")
        client_secret = os.getenv("GIS_CLIENT_SECRET")
        arcgis_url = "https://www.arcgis.com"  
        token_url = f"{arcgis_url}/sharing/rest/oauth2/token"

        params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        
        response = requests.post(token_url, params=params)
        
        if response.status_code == 200:
            token = response.json()["access_token"]    
            # print(f"Token: {token}")
            return token
        else:
            # print(f"Error: {response.status_code}")
            with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: token error {response.status_code}.\n")
            return None
    
    
    @staticmethod
    def address_to_zip(address):
        """use dataframe address to get zip code by using acrgis API

        Args:
            address (_type_): df['Street']

        Returns:
            _type_: zipcode (5 digits)
        """
        try: 
            token = DatabaseManager.get_token()
            if token is not None:                       
                url = "https://geocode-api.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
                params = {
                    "f": "pjson",
                    "singleLine": address,
                    "token": token
                }
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    address = data['candidates'][0]['address']
                    zipcode = address.split(',')[-1].strip()
                    if len(zipcode) == 5 and zipcode.isdigit():
                        return zipcode
                    else:
                        return None
                else:
                    with open('error_log.txt', 'a') as f:
                        f.write(f"Error processing address: {address}\n")
                    return None
            else:
                print("Missing token")
                pass
            
        except (KeyError, IndexError):
            with open('error_log.txt', 'a') as f:
                f.write(f"Error processing address: {address}\n")
            return None
        except requests.exceptions.RequestException as e:
            with open('error_log.txt', 'a') as f:
                f.write(f"Error processing address: {address}\nRequest error: {e}\n")
            return None
        except Exception as e:
            with open('error_log.txt', 'a') as f:
                f.write(f"Error processing address: {address}\nUnexpected error: {e}\n")
            return None
    
    
 


class EmailManager:
    def __init__(self, email_address, email_password):
        self.email_address = email_address
        self.email_password = email_password

    def send_email(self, subject, body, recipient):
        try:
            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = self.email_address
            msg['To'] = recipient
            msg.set_content(body)
            
            with smtplib.SMTP('smtp.office365.com', 587) as server: 
                server.starttls()
                server.login(self.email_address, self.email_password)
                server.send_message(msg)
            
            with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: Email sent successfully\n")
        except smtplib.SMTPException as e:
            with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: SMTP error occurred: {e}\n")
        except Exception as e:
            with open('log.txt', 'a') as f:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{current_time}: An error occurred while sending the email: {e}\n")
