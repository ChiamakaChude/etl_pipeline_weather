from datetime import datetime, timedelta
import os
import json

#some other functions useful for the ETL process

#set up temporary storage. It takes in the storage path+file name and the data to be stored
def setup_temp_storage(temp_storage, data_to_append):

    #check if the path exists
    if os.path.exists(temp_storage):
            with open(temp_storage, "r") as file:
                temp_data = json.load(file) #load the data and save to "temp_data" if storage file exists
    else:
        temp_data = []  #initialize if file doesn't exist

    # append new data to the list
    temp_data.append(data_to_append)

    #Write the updated list back to the JSON file
    with open(temp_storage, "w") as file:
        json.dump(temp_data, file)
    
    return temp_data

#code to convert time from UTC to normal time
def convert_time(timestamp):

    new_time = datetime.utcfromtimestamp(timestamp)

    return new_time.strftime('%Y-%m-%d %H:%M:%S')