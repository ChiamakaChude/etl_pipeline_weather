from datetime import datetime, timedelta
import logging
import pandas as pd

from weather_etl.scripts.database_connection import connect_db

from weather_etl.config.config import server
from weather_etl.config.config import database
from weather_etl.config.config import driver

from weather_etl.scripts.etl_functions import convert_time

#convert from meter/sec to km/hr
def convert_speed(speed):

    new_speed = speed * 3.6

    return new_speed

def check_columns(dataframe):

    try:
        logging.info("Checking data columns against database")

        connection = connect_db(server, database, driver)
        cursor = connection.cursor()

        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'WeatherData'
          AND COLUMN_NAME NOT IN (
              SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE TABLE_NAME = 'WeatherData' AND CONSTRAINT_NAME LIKE 'PK_%'
          );

        """
        cursor.execute(query)
        columns = cursor.fetchall()  # Fetch the first result row

        db_columns = []

        for column in columns:
            db_columns.append(column[0])

        if len(dataframe.columns) == len(db_columns):
            print("Column length matches with database")
            logging.info("Column length in transformed data matches with database")

            for column in dataframe.columns:

                if column not in db_columns:
                    print(f"{column} is not in the database")
                    logging.warning(f"{column} from raw data is not in the database")
                    return None

        connection.close()
        logging.info("All columns match")

        return dataframe
        
    except Exception as e:
        logging.error(f"Error occurred while checking the database: {e}")

def validate_and_clean(dataframe):
    logging.info(f"{dataframe.isna().sum().sum()} null values in data")
    logging.info(f"{dataframe.duplicated().sum()} duplicates in data")

    if dataframe.duplicated().sum() !=0:
        dataframe = dataframe.drop_duplicates()
        logging.info("Duplicates removed")

    dataframe.loc[:, 'city'] = dataframe['city'].str.title()
    dataframe.loc[:, 'weather'] = dataframe['weather'].str.lower()


    current_time_utc = datetime.utcnow().timestamp()
    # Calculate the time 1 hour ago as a timestamp
    one_hour_ago_utc = (datetime.utcnow() - timedelta(hours=1)).timestamp()

    one_hr = convert_time(one_hour_ago_utc)

    # Check if each timestamp is from the last 1 hour

    all_within_last_hour = dataframe['timestamp'].apply(lambda x: x >= one_hr).all()

    if all_within_last_hour == True:
        logging.info("All data is within the last hour")
    else:
        logging.warning("All data is not within the last hour")
    
    return dataframe

#data transformation
#data transformation code. takes in all batched
def transform_weather(raw_data):
    
    
    if not raw_data: #if extracted data is none
        logging.warning(f"No raw data received for transformation.")
        return None
    
    try:
        logging.info("Starting weather data transformation...")
        
        dataframe = pd.DataFrame()
        
        for data in raw_data: #loop through each batch
            transformed_data_dict = {} #initilise empty dictionary to store transformed data

            for city in data: #loop through to get data per city in the batch

                #extract useful fields
                transformed_data = {
                    "extraction_time": data[city].get("extraction_time", {}),
                    "city": city,
                    "temperature": data[city].get("main", {}).get("temp", None), #"None" if key does not exist
                    "feels_like": data[city].get("main", {}).get("feels_like", None), #"None" if key does not exist
                    "humidity": data[city].get("main", {}).get("humidity", None), #"None" if key does not exist
                    "wind_speed": data[city].get("wind", {}).get("speed", None),  # Default speed as 0
                    "weather": data[city].get("weather", [{}])[0].get("description", None), # "No description" if key does not exist
                    "timestamp": data[city].get("dt", None) #"None" if key does not exist
                }



                if transformed_data["wind_speed"] is not None: #if wind speed is not none
                    transformed_data["wind_speed"] = convert_speed(transformed_data["wind_speed"]) #convert from meter/sec to km/hr

                if transformed_data["timestamp"] is not None:
                    transformed_data["timestamp"] = convert_time(transformed_data["timestamp"]) #convert from UTC to human readable

                transformed_data_dict[city] = transformed_data #update dictionary with new transformed data for current city


            for city in transformed_data_dict: #loop through cities in the data

                city_data = pd.DataFrame([transformed_data_dict[city]]) #convert to dataframe
                dataframe = pd.concat([dataframe, city_data], ignore_index=True)
        
        
        #check for column match with database
        validate_data_db = check_columns(dataframe)
        if validate_data_db is None:
            return None
        
        transformed_data = validate_and_clean(dataframe) #validate and clean the data

        
        logging.info(f"Successfully transformed {len(dataframe.index)} records.")
        
    #error handling
    except Exception as e:
        
        logging.error(f"Error occurred during weather data transformation: {e}")
        raise
        return None
    
    return transformed_data