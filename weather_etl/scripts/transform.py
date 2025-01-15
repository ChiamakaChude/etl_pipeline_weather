import pandas as pd 
import logging

from weather_etl.scripts.transform_traffic import transform_traffic
from weather_etl.scripts.transform_weather import transform_weather

def transform(raw_weather_data, raw_traffic_data):

    tf_weather_data = transform_weather(raw_weather_data)

    tf_traffic_data = transform_traffic(raw_traffic_data)

    logging.warning("Merging traffic and weather data started...")

    if tf_traffic_data is not None and tf_traffic_data is not None:
        merged_data = pd.merge(tf_weather_data, tf_traffic_data, on=["city", "extraction_time"], how="outer")
        logging.warning("Merging traffic and weather data successful!!")
        return merged_data
    
    else:
        logging.warning("One or both datasets are None. Merging cannot be performed.")
        return None