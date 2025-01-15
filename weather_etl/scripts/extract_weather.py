import requests
import logging
import os
import json

from weather_etl.config.config import base_weather_url
from weather_etl.config.config import api_key_weather
from weather_etl.config.config import temp_storage_weather
from weather_etl.config.config import batch_size

from weather_etl.scripts.etl_functions import setup_temp_storage


def extract_weather(cities, batch_size, time):
    
    logging.info("Starting weather data extraction...")

    try:
        
        weather_city = {} #dictionary to contain extracted data
        count = 0
        
        for city in cities: #loop through cities list to get data for each city
            
            count += 1 #counter for how many times loop runs
            url = f"{base_weather_url}?lat={city['latitude']}&lon={city['longitude']}&appid={api_key_weather}&units=metric" #url
            response = requests.get(url) #get request response

            if response.status_code == 200: #check response status code
                weather_city[city["city"]] = {"extraction_time": time, **response.json()} #get data for current city and update the dictionary

            else:
                print(f"Failed to fetch data: {response.status_code}")
                logging.error(f"Failed to fetch data: {response.status_code}") #log error message
                return None
        
        #temp_data.append(weather_city)

        #load extracted data into temporary storage
        temp_data = setup_temp_storage(temp_storage_weather, weather_city)

        logging.info(f"Successfully extracted {count} records. Current batch size: {len(temp_data)}") #log number of records extracted

        if len(temp_data) >= batch_size:
            return temp_data #return extracted data if batch size is met
        else:
            return None
        
    except Exception as e: #error handling
    
        logging.error(f"Error occurred during data extraction: {e}")
        raise

#just to test extraction function
if __name__=="__main__":

    from weather_etl.config.config import server
    from weather_etl.config.config import database
    from weather_etl.config.config import driver
    from weather_etl.config.config import batch_size

    cities = ["London", "New York", "Paris", "Tokyo", "Lagos"] #cities to extract data for

    extract_weather(cities, batch_size)