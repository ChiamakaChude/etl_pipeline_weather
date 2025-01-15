from datetime import datetime, timedelta
import logging
import pandas as pd

from weather_etl.scripts.database_connection import connect_db

from weather_etl.config.config import server
from weather_etl.config.config import database
from weather_etl.config.config import driver

from weather_etl.scripts.etl_functions import convert_time

# code to transfom traffic data. Argument is the temp data - list of list of dictionaries
def transform_traffic(traffic_data):
    
    logging.info("Starting traffic data transformation...")

    if not traffic_data: #if extracted data is none
        logging.warning(f"No raw traffic data received for transformation.")
        return None
    
    try: 
        dataframe = pd.DataFrame() #dataframe to append transformed data to
        
        for data in traffic_data: #loop through each batch in the data

            transformed_data_dict = {} # dictionary to store transformed data. Resets in each batch loop

            for city in data: #loop through each city in the batch

                #gets the relevant data from the raw data
                traffic_result = {"extraction_time": data[city]["extraction_time"],
                                "city": city,
                                "longitude": data[city]["longitude"],
                                "latitude": data[city]["latitude"],
                                "traffic_speed": data[city]["flowSegmentData"]["currentSpeed"],
                                "traffic_congestion": data[city]["flowSegmentData"]["freeFlowSpeed"],
                                "currentTravelTime": data[city]["flowSegmentData"]["currentTravelTime"],
                                "freeFlowTravelTime": data[city]["flowSegmentData"]["freeFlowTravelTime"],
                                "confidence": data[city]["flowSegmentData"]["confidence"],
                                "roadClosure": data[city]["flowSegmentData"]["roadClosure"]}
                
                # Append the dictionary with each result. Since key is the same in each batch,it replaces the value pair in each batch loop
                transformed_data_dict[city] = traffic_result
            
            # Convert the dictionary to a dataframe
            transformed_data_df = pd.DataFrame.from_dict(transformed_data_dict, orient='index')

            # Conactenate the data with initialised dataframe
            dataframe = pd.concat([dataframe, transformed_data_df], ignore_index=True)

        dataframe.reset_index(inplace=True) #reset dataframe index
        dataframe = dataframe.drop(columns="index") #drop index column

        logging.info(f"Successfully transformed {len(dataframe.index)} records.")

    except Exception as e:
        logging.error(f"Error occurred during traffic data transformation: {e}")
            
    return dataframe