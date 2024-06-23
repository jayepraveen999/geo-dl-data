import datetime
import os
import yaml
import time
import random
import argparse
import pandas as pd
import geopandas as gpd
from fire_masks.fire_labels import FireLabels
from utils import get_time_series, get_random_timestamps
import warnings
import logging as log
from concurrent.futures import ThreadPoolExecutor

log_file = f"reprocess_data_2/fire_labels/generate_fire_labels.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

warnings.filterwarnings("ignore")

# Load the configuration files
with open("config/fire_labels_auth.yml", 'r') as stream:
    try:
        CONFIG = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        log.error(exc)

# main function to get the data
def main(fire_labels: FireLabels, aoi:dict, year:int, seed: int):

    # Define start and end dates for the year
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)

    # Iterate through the days in the year
    current_date = start_date

    # Define a dictionary to store the data for each day with clusters and fire_events metadata
    fires = []
    while current_date <= end_date:

        # get 4 random time stamps for each of the 6 hours intervals in a day
        time_series = get_random_timestamps(timestamps=TIMESTAMPS, intervals=INTERVALS)
        
        # Print the current date in the "YYYY-MM-DD-HHMM" format using timeseries generated above
        for HHMM in time_series:
            DATE = f"{current_date.strftime('%Y-%m-%d-')}{HHMM}"
            log.info(f"Requesting data for {DATE}")

            # request clusters data within the timeframe
            clusters_data = fire_labels.get_clusters_data(coordinates=aoi, n_minutes= 10, date=DATE)
            if clusters_data is None:
                continue
            preprocess_clusters_data = fire_labels.preprocess_clusters(clusters_data, DATE)

            if preprocess_clusters_data is None:
                continue
            
            # request fire events data within the timeframe
            for i in preprocess_clusters_data["cluster_id"]:
                fire_events_data = fire_labels.get_fire_events_data(id=i)
                cluster_oldest_time = datetime.datetime.strptime((preprocess_clusters_data[preprocess_clusters_data["cluster_id"]==i]["cluster_oldest_acquisition"]).iloc[0],"%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d-%H%M")
                if fire_events_data is None:
                    continue
                
                preprocess_fire_events_data = fire_labels.preprocess_fire_events(fire_events_data, DATE, TIMEDELTA, i, cluster_oldest_time)
                if preprocess_fire_events_data is None:
                    continue

                # merge the clusters and fire events data
                fire_clusters_df = pd.merge(preprocess_clusters_data, preprocess_fire_events_data, left_on="cluster_id", right_on="fire_cluster_id", how="right")
                fires.append(fire_clusters_df)  
            
        # Move to the next day
        current_date += datetime.timedelta(days=1)
    if len(fires) == 0:
        log.info(f"No data for {year}")
        return
    labelled_year_data = gpd.GeoDataFrame(pd.concat(fires,ignore_index=True))
    log.info(f"Total no. of fire_events for {year} are {len(labelled_year_data)}")
    labelled_year_data.to_file(f"reprocess_data_2/fire_labels/ten_minute/{year}_labelled_data_{seed}.geojson", driver="GeoJSON")


def combine_all_years(YEARS:list, seed:int):

    labelled_year_data_list = []
    for year in YEARS:
        labelled_year_data_list.append(gpd.read_file(f"reprocess_data_2/fire_labels/ten_minute/{year}_labelled_data_{seed}.geojson"))
    # merge the data for all the years
    labelled_data_all_years = gpd.GeoDataFrame(pd.concat(labelled_year_data_list,ignore_index=True))
    log.info(f"Total no. of fire_events for all years are {len(labelled_data_all_years)}")
    labelled_data_all_years.to_file(f"reprocess_data_2/fire_labels/ten_minute/2020_2021_2022_combined_{seed}.geojson", driver="GeoJSON")


if __name__ == "__main__":

    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--TIMESTAMPS", type=int, default=4, help="No of time stamps to be considered for each of 6 hours intervals in a day")
    # parser.add_argument("--SEED", type=int, default=42, help="Random seed to pick these time stamps")
    args = parser.parse_args()

    # Define the CONSTANTS

    TIMESTAMPS = args.TIMESTAMPS
    TIMEDELTA = 180
    API_KEY = CONFIG["API_KEY"]
    AOI = CONFIG["AOI"]
    YEARS = CONFIG["YEARS"]
    # INTERVALS = CONFIG["INTERVALS"]
    INTERVALS = CONFIG["INTERVALS_2"]

    SEED = int(time.time())
    # SEED = 1703273207 # use this for reproducibility

    random.seed(SEED)
    log.info(f"SEED used is {SEED}")

    # Create the FireLabels object
    fire_labels = FireLabels(api_key=API_KEY)

    # get fire labels for each year
    with ThreadPoolExecutor(max_workers=3) as executor:
        for year in YEARS:
            if os.path.exists(f"reprocess_data_2/fire_labels/{year}_labelled_data_{SEED}.geojson"):
                log.info(f"reprocess_data_2/fire_labels/{year}_labelled_data_{SEED}.geojson already exists")
                continue
            executor.submit(main, fire_labels, AOI, year, SEED)

    combine_all_years(YEARS, SEED)