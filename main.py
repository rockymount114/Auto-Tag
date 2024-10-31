from dotenv import load_dotenv
import os
from DB import DatabaseManager
from query import QUERY

load_dotenv()

SERVER_ADDRESS = os.getenv('SERVER_NAME')
DATABASENAME = os.getenv('DATABASE')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

db_manager = DatabaseManager(SERVER_ADDRESS, DATABASENAME, DB_USERNAME, DB_PASSWORD)
 
if __name__ == "__main__":  
    df = db_manager.fetch_data(db_manager.engine, QUERY) 
    print(df)
    
    