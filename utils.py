import random
import pyproj
import boto3
import json
from datetime import datetime, timedelta
from botocore.client import Config
from botocore import UNSIGNED
import os


def get_time_series(timestamps:int, intervals:list[list],seed:int) -> dict:
    """
    Returns a dictionary of timestamps for each interval
    """
    n = len(intervals)

    random.seed(seed)
    random_indices = [random.randint(0,len(intervals[0])-1) for _ in range(n)]
    
    time_series_indices = {}
    for i,enum in enumerate(intervals):
        if random_indices[i] > timestamps:

            time_series_indices[i] = enum[random_indices[i]-timestamps:random_indices[i]]
        else:
            random_indices[i] = timestamps
            time_series_indices[i] = enum[random_indices[i]-timestamps:random_indices[i]]
    
    return time_series_indices

def get_random_timestamps(timestamps:int, intervals:list[list]) -> list:

    """
    Returns random timestamps for each of interval defined in intervals. SEED is set at the top level
    """
    n = len(intervals)
    random_indices = [random.randint(0,len(intervals[0])-1) for _ in range(n)]
    
    time_series_indices = []
    for i,enum in enumerate(intervals):

        if random_indices[i] > timestamps:
            time_series_indices.append(enum[random_indices[i]])

        else:
            random_indices[i] = timestamps
            time_series_indices.append(enum[random_indices[i]])
    
    return time_series_indices

def get_child_timestamps(timestamp_str:str) -> list[str]:
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

def get_h8_proj4_string():
    """
    Returns the proj4 string for himawari8 projection
    """
    return "+proj=geos +over +lon_0=140.700 +lat_0=0.000 +a=6378137.000 +f=0.0033528129638281333 +h=35785863.0"

def get_h8_proj_transformer(epsg: str):
    """
    Returns a transformer for the given epsg code onto himawari8 projection
    """

    wgs84 = pyproj.CRS(epsg)
    himawari_crs = pyproj.CRS.from_proj4("+proj=geos +over +lon_0=140.700 +lat_0=0.000 +a=6378137.000 +f=0.0033528129638281333 +h=35785863.0")

    return pyproj.Transformer.from_crs(wgs84,himawari_crs, always_xy=True).transform

def get_h8_fldk_data(timestamp: str, path: str):
    """
    Downloads the fldk files for all bands for the given timestamp
    args:
        timestamp: str (timestamp for which data to be downloaded ex: "2021/04/30/1650")
        path: str   (path to save the data ex: "data/band_analysis/")
    
    """

    timestamp = timestamp
    fldk_dir = 'AHI-L1b-FLDK'
    fldk_url = f'{fldk_dir}/{timestamp}'
    bucket_name = 'noaa-himawari8'
    s3 = boto3.client("s3",config=Config(signature_version=UNSIGNED),region_name='us-east-1')
    fldk_response = s3.list_objects(Bucket=bucket_name, Prefix=fldk_url)
    data_files = [obj['Key'] for obj in fldk_response.get('Contents', [])]

    for file_key in data_files:
        path = path
        if not os.path.exists(path):
            os.makedirs(path)
        local_file_path = path + file_key.split('/')[-1]  # Specify local download path
        if not os.path.exists(local_file_path):
            s3.download_file(bucket_name, file_key, local_file_path)
            print(f"Downloaded {file_key.split('/')[-1]} to {local_file_path}")
        else:
            print(f"File {local_file_path} already exists")

    return 

def get_2022_timestamps():

    # using locally saved unique timestamps 
    with open("data/fire_masks/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)

    # remove timestamps for 2022 due to storage constraints | processing 2022 timestamps separately
    timestamps_2022 = []
    for timestamp in timestamps:
        if timestamp.split("/")[0] == "2022":
            timestamps_2022.append(timestamp)

    return timestamps_2022