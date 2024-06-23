import random
import pyproj
import boto3
import json
import rasterio
import numpy as np
import geopandas as gpd
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

def create_empty_h8_mask():
    """
    Create an empty raster with the same extent as the aoi 
    """
    if not os.path.exists("data/himawari8/empty_mask_h8_aoi_updated.tif"):
        # read aoi
        aoi_h8 = gpd.read_file("02_input_data/aoi_h8_updated.geojson")


        # mask the raster with aoi extent: take a sample file from himawari8
        with rasterio.open('data/himawari8/sample_data_B05_20220101_004000.tif') as src:
            masked_raster, masked_transform = rasterio.mask.mask(src, aoi_h8["geometry"], crop=True)
            masked_meta = src.meta

        masked_meta.update({"driver": "GTiff",
                        "height": masked_raster.shape[1],
                        "width": masked_raster.shape[2],
                        "transform": masked_transform})

        # create an empty raster similar to masked raster
        masked_empty_raster = np.zeros_like(masked_raster)
        masked_empty_raster[1,:,:] = masked_raster[1,:,:]

        with rasterio.open("data/himawari8/empty_mask_h8_aoi_updated.tif", "w", **masked_meta) as dest:
            dest.write(masked_empty_raster)

        return
    else:
        print("Empty raster already exists")
    return
