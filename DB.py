import pandas as pd

from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pyodbc
from dotenv import load_dotenv
import os

import smtplib
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
    
    
 


# class EmailManager:
#     def __init__(self, email_address, email_password):
#         self.email_address = email_address
#         self.email_password = email_password

#     def send_email(self, subject, body, recipient):
#         try:
#             # Set up the email server and send the email
#             msg = EmailMessage()
#             msg['Subject'] = subject
#             msg['From'] = self.email_address  # Ensure this is set
#             msg['To'] = recipient
#             msg.set_content(body)

#             # Update the SMTP server and port here
#             with smtplib.SMTP('smtp.office365.com', 587) as server:  # Use your SMTP server
#                 server.starttls()
#                 server.login(self.email_address, self.email_password)
#                 server.send_message(msg)
            
#             with open('log.txt', 'a') as f:
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 f.write(f"{current_time}: Email sent successfully\n")
#         except smtplib.SMTPException as e:
#             with open('log.txt', 'a') as f:
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 f.write(f"{current_time}: SMTP error occurred: {e}\n")
#         except Exception as e:
#             with open('log.txt', 'a') as f:
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 f.write(f"{current_time}: An error occurred while sending the email: {e}\n")
