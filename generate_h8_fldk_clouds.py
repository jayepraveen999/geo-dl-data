import os
import json
import time
from datetime import datetime, timedelta
import geopandas as gpd
import boto3
from botocore.client import Config
from botocore import UNSIGNED
from concurrent.futures import ThreadPoolExecutor
import logging as log
log_file = f"generate_fldk_clouds.txt"  # Path to the log file

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

def read_timestamps(labels_path:str) -> list:
    """
    Returns a list of unique timestamps of the labels
    """
    labels = gpd.read_file(labels_path)
    time_stamps = labels["date"].dt.strftime('%Y/%m/%d/%H%M/')
    unique_time_stamps= list(time_stamps.unique().astype(str))
    

    return unique_time_stamps
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
         # Filter objects based on the naming convention
        fldk_files = [obj['Key'] for obj in fldk_response.get('Contents', [])
                if '_B07_' in obj['Key'] or '_B11_' in obj['Key'] or '_B12_' in obj['Key'] or '_B13_' in obj['Key'] or '_B14_' in obj['Key'] or '_B15_' in obj['Key']]
        data_files.extend(fldk_files)

        # Download the cloud product data only for the last timestamp
        if child_timestamp == timestamp:
            cloud_url = f'{CLOUD_PRODUCT_DIR}/{child_timestamp}'
            cloud_response = s3.list_objects(Bucket=BUCKET_NAME, Prefix=cloud_url)
            cloud_files = [obj['Key'] for obj in cloud_response.get('Contents', [])
                    if '-CMSK_' in obj['Key'] or '_CLOUD_MASK_' in obj['Key']]
            data_files.extend(cloud_files)
    
        # Download desired files
        for file_key in data_files:
            path = f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/"
            if not os.path.exists(path):
                os.makedirs(path)
            local_file_path = path + file_key.split('/')[-1]  # Specify local download path
            if not os.path.exists(local_file_path):
                s3.download_file(BUCKET_NAME, file_key, local_file_path)
                log.info(f"Downloaded {file_key.split('/')[-1]} to {local_file_path}")
            else:
                log.info(f"File {local_file_path} already exists")

        
   
if __name__ == "__main__":



    # Fire labels to get the timestamps used to download h8 data

    # labels_path = 'data/fire_labels/combined_data/finalized_data/2021_2022_combined_12_preprocessed.geojson'
    # timestamps = read_timestamps(labels_path)

    # using locally saved unique timestamps for seed 12 
    with open("reprocess_data_2/fire_labels/ten_minute/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)
    log.info(f"Downloading FLDK and CMSK for {len(timestamps)} timestamps")
    t = time.time()
    with ThreadPoolExecutor(max_workers=32) as executor:
        for timestamp in timestamps:
            executor.submit(main, timestamp)
    log.info(f"Downloaded all files in {(time.time()-t)/60} minutes")
    # main(BUCKET_NAME, FLDK_DIR, CLOUD_PRODUCT_DIR, timestamps)






