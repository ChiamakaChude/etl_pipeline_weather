import requests
import pandas as pd
from datetime import datetime
import logging
import pyodbc
import os

from weather_etl.config.config import log_file
from weather_etl.config.config import server
from weather_etl.config.config import database
from weather_etl.config.config import driver
from weather_etl.config.config import batch_size
from weather_etl.config.config import cities

from weather_etl.scripts.extract import extract
from weather_etl.scripts.transform import transform

from weather_etl.scripts.database_connection import connect_db
from weather_etl.scripts.load import load_weather


"""logging.basicConfig(
    filename = log_file,   # Log file name
    level = logging.DEBUG,          # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format = "%(asctime)s - %(levelname)s - %(message)s"  # Log format (timestamp, log level, message)
)"""


logging.basicConfig(
    level=logging.DEBUG,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",  # Include filename in the logs
    handlers=[
        logging.FileHandler(log_file),  # Write logs to the log file
        logging.StreamHandler()  # Write logs to the console (Airflow captures this)
    ]
)


#ETL execution function
#ETL execution function
def run_etl(cities, server, database, driver, batch_size):

    logging.info("ETL process started...")

    try:
        
        raw_weather_data, raw_traffic_data = extract(cities, batch_size) #call extraction function
        
        final_data = transform(raw_weather_data, raw_traffic_data) #call transformation function
        
        if final_data is not None:
        
            connection = connect_db(server, database, driver) #call database connection function

             #if data was transformed and is not null
            weather_df = load_weather(final_data, connection) #call load function

            logging.info("ETL process completed successfully.")
            
            return weather_df #return the final data in a dataframe
        return None
        #print(final_data)
        
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise
                
        
if __name__ == "__main__": 
    run_etl(cities, server, database, driver, batch_size)