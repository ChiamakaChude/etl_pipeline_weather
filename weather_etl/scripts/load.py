import pyodbc
import pandas as pd
import logging
import os
import numpy as np
import json

from weather_etl.config.config import temp_storage_traffic, temp_storage_weather
from weather_etl.config.config import server
from weather_etl.config.config import database
from weather_etl.config.config import driver


from weather_etl.scripts.database_connection import connect_db

def clear_temp_storage(temp_storage_file):

    if os.path.exists(temp_storage_file):
        with open(temp_storage_file, "w") as file:
            os.remove(temp_storage_file) 
        logging.info("Temporary batch storage deleted.")
    else:
        logging.info("Temporary storage file does not exist. No action taken.")


def check_columns(dataframe):

    try:
        table_name = "Weather_Traffic_Data" #name of database table

        logging.info("Checking data columns against database")

        connection = connect_db(server, database, driver) #establish database connection
        cursor = connection.cursor()

        query = f"""
        SELECT COUNT(name) FROM sys.tables WHERE name='{table_name}'

        """
        cursor.execute(query) #execute the query
        result = cursor.fetchone()  #fetch the first result row
        connection.commit()
        
        if result[0] != 0: #if the result is not 0 it means the table exists
            return dataframe
        else: #otherwise the table does not exist so create table

            logging.info(f"Table {table_name} does not exist. Creating table...")

            columns = dataframe.columns # get column names from dataframe. this will be used for the db columns
            column_definitions = [] # column definitions to be used for the sql create table query
            column_definitions.append("ID INT IDENTITY(1,1) PRIMARY KEY") #set primary key and append to definitions list

            # mapping of dataframe datatypes to database column type format
            data_type_mapping = {
                'int64': 'INT',
                'float64': 'FLOAT',
                'object': 'VARCHAR(255)',  # Adjust length as needed
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'VARCHAR(255)'
            }

            #loop through the columns and their types in the dataframe and zip together
            for column, dtype in zip(columns, dataframe.dtypes):
                #get the sql data type mapping for the dataframe data type
                sql_dtype = data_type_mapping.get(str(dtype), 'VARCHAR(255)')  #default to VARCHAR if type not found
                column_definitions.append(f"{column} {sql_dtype}") # combine the column name and sql data type and append to the definitions list

            # setup the sql create table query
            create_table_query = f"CREATE TABLE {table_name} (\n"
            create_table_query += ",\n".join(column_definitions)
            create_table_query += "\n);"

            cursor.execute(create_table_query)
            connection.commit()
            
            logging.info(f"Table {table_name} created successfully")


        connection.close()
        logging.info("All columns match")

        return dataframe
        
    except Exception as e:
        logging.error(f"Error occurred while checking the database: {e}")



#load data into database
def load_weather(data, connection):
    
    try:
        logging.info("Starting data load.")
        
        #object to interract with database
        cursor = connection.cursor()
        check_columns(data)

        data_dict = data.to_dict('records') #convert data to dictionary from dataframe before load

        # convert NaN to None before loading into DB. DB throws an error when inserting NaN
        def nan_to_none(record):
            for key, value in record.items():
                if isinstance(value, float) and np.isnan(value):  # check if the value is NaN (for numeric columns)
                    record[key] = None
                elif isinstance(value, str) and value == '':  # check if the value is an empty string (for text columns)
                    record[key] = None
            return record

        # Apply the nan_to_none function to all records
        data_dict = [nan_to_none(record) for record in data_dict]

        insert_query = """
        INSERT INTO Weather_Traffic_Data 
        (extraction_time, city, temperature, feels_like, humidity, wind_speed, weather, timestamp, longitude, latitude, traffic_speed, traffic_congestion, currentTravelTime, freeFlowTravelTime, confidence, roadClosure)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Loop through each dictionary in the list
        for record in data_dict:
            #logging.info(f"Inserting {record['city']}...")  # Log the insertion

            # Insert the record into the database
            cursor.execute(insert_query, (
                record['extraction_time'], 
                record['city'], 
                record['temperature'], 
                record['feels_like'], 
                record['humidity'], 
                record['wind_speed'], 
                record['weather'], 
                record['timestamp'], 
                record['longitude'], 
                record['latitude'], 
                record['traffic_speed'], 
                record['traffic_congestion'], 
                record['currentTravelTime'], 
                record['freeFlowTravelTime'], 
                record['confidence'], 
                record['roadClosure']
            ))

            # Commit the transaction
        connection.commit()
            #logging.info(f"{record['city']} insert successful")
        
        print("Records inserted successfully.")
        logging.info(f"Successfully loaded {len(data)} records into the database.")

        #clear the temp storage after inserting into the db
        clear_temp_storage(temp_storage_traffic)
        clear_temp_storage(temp_storage_weather)
        

    except Exception as e:
        logging.error(f"Error occurred during data load: {e}")
        raise
        
    return data #return the dataframe