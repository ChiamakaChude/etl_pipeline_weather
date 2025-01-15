@echo off
REM Add a timestamp and indicate that the batch file is starting
echo %date% %time% - INFO - Starting Weather ETL Script from Batch File >> C:\Users\amych\OneDrive\Documents\Projects\Data_Engineering\logs\weather_etl.log

REM Run the Python script and capture both standard output and errors in the log file
python C:\Users\amych\OneDrive\Documents\Projects\Data_Engineering\main.py >> C:\Users\amych\OneDrive\Documents\Projects\Data_Engineering\logs\weather_etl.log

REM Add a timestamp and indicate that the batch file has finished
echo %date% %time% - INFO - Finished Weather ETL Script from Batch File >> C:\Users\amych\OneDrive\Documents\Projects\Data_Engineering\logs\weather_etl.log