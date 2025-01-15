import requests
import logging
import os
import json

from weather_etl.config.config import base_traffic_url
from weather_etl.config.config import api_key_traffic
from weather_etl.config.config import temp_storage_traffic
from weather_etl.config.config import batch_size

from weather_etl.scripts.etl_functions import setup_temp_storage


#code to extract traffic data. takes in the dictionary of cities with long and lat, batch size and extraction time
def extract_traffic_data(cities, batch_size, time):
    
    logging.info("Starting traffic data extraction...")
    
    try:
        
        raw_traffic_data = {} #create emoty dictionary to store extracted data
        count = 0 #to count number of extracted records

        for city in cities: #loop through cities in the dictionary
            
            count += 1 #inrement the count
            
            params = {
                "point": f"{city['latitude']},{city['longitude']}", # get lon and lat from cities dictionary
                "key": api_key_traffic # traffic API key
            }

            response = requests.get(base_traffic_url, params=params) # API call

            if response.status_code == 200:

                traffic_data = response.json() # get response data

                #create dictionary with city name as key and value is another dictionary
                raw_traffic_data[city["city"]] = {"extraction_time": time, "longitude": city["longitude"], "latitude": city["latitude"], **traffic_data}

                #just to see the data
                traffic_speed = traffic_data["flowSegmentData"]["currentSpeed"]
                traffic_congestion = traffic_data["flowSegmentData"]["freeFlowSpeed"]
                currentTravelTime = traffic_data["flowSegmentData"]["currentTravelTime"]
                freeFlowTravelTime = traffic_data["flowSegmentData"]["freeFlowTravelTime"]
                confidence = traffic_data["flowSegmentData"]["confidence"]
                roadClosure = traffic_data["flowSegmentData"]["roadClosure"]

                #print(f"City: {city['city']}, Longitude: {city['longitude']}, Traffic Speed: {traffic_speed} km/h, Congestion Level: {traffic_congestion}")
            else:
                # log city that traffic data cant be extracted for
                logging.error(f"Error: {response.status_code} for city {city['city']}. Longitude: {city['longitude']}")
                #return None

        #load all extracted data into the temporary storage
        temp_data = setup_temp_storage(temp_storage_traffic, raw_traffic_data)

        logging.info(f"Successfully extracted {count} records. Current batch size: {len(temp_data)}") #log number of records extracted

        if len(temp_data) >= batch_size:
            return temp_data #return extracted data if batch size is met
        else:
            return None # else return nothing
        
    except Exception as e:
        
        logging.error(f"Error occurred during traffic data extraction: {e}")