import json
import time
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import boto3
from botocore.client import Config
from botocore import UNSIGNED
from concurrent.futures import ThreadPoolExecutor
import logging as log
log_file = f"finalized_labels_with_fldk_cmsk_availability.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# Define the S3 bucket and directory
BUCKET_NAME = 'noaa-himawari8'
FLDK_DIR = 'AHI-L1b-FLDK'
CLOUD_PRODUCT_DIR = 'AHI-L2-FLDK-Clouds'

def get_child_timestamps(timestamp_str):
    """
    Returns the last three 10-minute intervals for the given timestamp
    """
    # Convert the timestamp string to a datetime object
    timestamp = datetime.strptime(timestamp_str, '%Y/%m/%d/%H%M/')

    # Calculate the last three 10-minute intervals
    intervals = []
    for i in range(1, 4):
        new_time = timestamp - timedelta(minutes=i * 10)
        intervals.append(new_time.strftime('%Y/%m/%d/%H%M/'))
    intervals.append(timestamp_str)

    return sorted(intervals)

def main(timestamp:str):
    """ 
    Downloads the fldk and cloud product data for the given timestamps

    """
    s3 = boto3.client("s3",config=Config(signature_version=UNSIGNED),region_name='us-east-1')

    child_timestamps = get_child_timestamps(timestamp)

    for child_timestamp in child_timestamps:

        data_files = []
        # Download the fldk data
        fldk_url = f'{FLDK_DIR}/{child_timestamp}'
        fldk_response = s3.list_objects(Bucket=BUCKET_NAME, Prefix=fldk_url)
        if fldk_response.get('Contents') is None:
            log.info(f"No FLDK data for child_timestamp {child_timestamp} with main timestamp {timestamp}")
            delete_timestamps.append(timestamp)
            continue
         # Filter objects based on the naming convention
        fldk_files = [obj['Key'] for obj in fldk_response.get('Contents', [])
                if '_B07_' in obj['Key'] or '_B11_' in obj['Key'] or '_B12_' in obj['Key'] or '_B13_' in obj['Key'] or '_B14_' in obj['Key'] or '_B15_' in obj['Key']]
        data_files.extend(fldk_files)

        # Download the cloud product data only for the last timestamp
        if child_timestamp == timestamp:
            cloud_url = f'{CLOUD_PRODUCT_DIR}/{child_timestamp}'
            cloud_response = s3.list_objects(Bucket=BUCKET_NAME, Prefix=cloud_url)
            if cloud_response.get('Contents') is None:
                log.info(f"No CLOUD data for timestamp {timestamp}")
                delete_timestamps.append(timestamp)
                continue
            cloud_files = [obj['Key'] for obj in cloud_response.get('Contents', [])
                    if '-CMSK_' in obj['Key'] or '_CLOUD_MASK_' in obj['Key']]
            data_files.extend(cloud_files)
    


        
   
if __name__ == "__main__":


    with open("reprocess_data_2/fire_labels/ten_minute/unique_dates_ten_minute.json") as json_file:
        timestamps = json.load(json_file)
    
    delete_timestamps = []
    t = time.time()
    with ThreadPoolExecutor(max_workers=16) as executor:
        for timestamp in timestamps:
            executor.submit(main, timestamp)
    delete_timestamps = list(set(delete_timestamps))
    log.info(f"Got all the deleted timestamps: {delete_timestamps}")
    log.info(f"No of timestamps to be deleted: {len(delete_timestamps)}")

    labels_data = gpd.read_file("reprocess_data_2/fire_labels/ten_minute/2020_2021_2022_combined_1703273207_ten_minute_preprocessed.geojson")
    labels_data["date"] = labels_data["date"].dt.strftime("%Y/%m/%d/%H%M/")
    labels_data = labels_data[~labels_data["date"].isin(delete_timestamps)]

    unique_dates = labels_data["date"].unique().tolist()

    # convert the date column to datetime format
    labels_data["date"] = pd.to_datetime(labels_data["date"], format="%Y/%m/%d/%H%M/")

    # save the updated labels data to a new file
    labels_data.to_file("reprocess_data_2/fire_labels/ten_minute/2020_2021_2022_combined_1703273207_ten_minute_preprocessed_finalized.geojson", driver="GeoJSON")
    log.info(f"Saved the updated labels data to a new file")

    # save unique dates to a new file
    file_path = f"reprocess_data_2/fire_labels/ten_minute/unique_dates_ten_minute_finalized.json"
    with open(file_path, 'w') as json_file:
        json.dump(unique_dates, json_file)

    log.info(f"Saved the updated unique dates to a new file")




