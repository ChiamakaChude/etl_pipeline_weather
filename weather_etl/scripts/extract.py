import requests
import logging
import os
import json
from datetime import datetime, timedelta



from weather_etl.config.config import base_traffic_url
from weather_etl.config.config import api_key_traffic
from weather_etl.config.config import temp_storage_traffic
from weather_etl.config.config import batch_size

from weather_etl.config.config import base_weather_url
from weather_etl.config.config import api_key_weather
from weather_etl.config.config import temp_storage_weather

from weather_etl.config.config import cities

from weather_etl.scripts.etl_functions import convert_time

from weather_etl.scripts.extract_weather import extract_weather
from weather_etl.scripts.extract_traffic import extract_traffic_data

# call both traffic and weather extraction functions. takes in the batch size and dictionary of cities
def extract(cities, batch_size):
    
    # get current time extraction starts
    current_time_utc = datetime.utcnow().timestamp()

    #convert the time from UTC to normal time format. This is done so this time and time from APIs are consistent formats
    current_time = convert_time(current_time_utc) 
    
    #call weather extraction function
    raw_weather_data = extract_weather(cities, batch_size, current_time)

    # Call traffic extraction function
    raw_traffic_data = extract_traffic_data(cities, batch_size, current_time)
    
    return raw_weather_data, raw_traffic_data