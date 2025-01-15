import os
import pandas as pd

current_file_path = os.path.abspath(__file__)

# Get the project root directory (adjust based on your project structure)
project_root = os.path.dirname(os.path.dirname(current_file_path))  # Two levels up

# Construct the relative path to the logs folder
log_file = os.path.join(project_root, "logs", "weather_etl.log")

# Ensure the logs directory exists
os.makedirs(os.path.dirname(log_file), exist_ok=True)

#api keys for weather and traffic data
api_key_weather = ""
api_key_traffic = ""

#base url for weather and traffic
base_weather_url = "https://api.openweathermap.org/data/2.5/weather"
base_traffic_url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

batch_size = 4 #number of batches of data to collect before transform and load

#temporary storage to store data batches for weather and traffic data
temp_storage_weather = os.path.join(project_root, "data", "temp_storage_weather.json")
temp_storage_traffic = os.path.join(project_root, "data", "temp_storage_traffic.json")

#csv file with list of places in London with longitude and latitude that's used to retrieve weather and traffic data
data = pd.read_csv(os.path.join(project_root, "data","london_places.csv"))
cities = data.to_dict(orient="records") #convert the dataframe to a list of dictionaries
cities = cities[:30] 

#database credentials
server = '192.168.0.197'
database = 'Weather'
driver = 'ODBC Driver 17 for SQL Server'
username='sa'
password='sa'