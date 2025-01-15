import pyodbc
import logging

from weather_etl.config.config import server, database, driver, username, password,log_file
#from weather_etl.config.config import database
#from weather_etl.config.config import driver

#Code to connect to the database

#Connect to database
def connect_db(server, database, driver):
    
    try:
        logging.info(f"Connection to database '{database}'...")
        #Initial connection to the server
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=no;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Check if the database exists
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = ?", database)
        db_exists = cursor.fetchone()

        if not db_exists:
            #If database does not exist, create it
            logging.warning(f"Database '{database}' does nto exist. Creating it...")
            
            cursor.execute(f"CREATE DATABASE {database}") #execute create query

            logging.info(f"Database '{database}' created successfully.")
            
        else:
            logging.info(f"Connection to database '{database}' successful!!!")
            
        return connection #return the database connection
        
    except Exception as e:
        #If there is an error
        print(f"Unexpected error occured {e}")
        logging.error(f"Error occurred during database connection: {e}")
        return None

#Just to test database connection
if __name__ == "__main__": 
    logging.basicConfig(
    filename = log_file,   # Log file name
    level = logging.DEBUG,          # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format = "%(asctime)s - %(levelname)s - %(message)s"  # Log format (timestamp, log level, message)
)
    connection = connect_db(server, database, driver)