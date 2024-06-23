import requests
import datetime
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from requests.models import Response
from pytz import UTC
import warnings
import logging as log
log_file = "generate_fire_labels.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

warnings.filterwarnings("ignore")


class FireLabels():
    def __init__(self, api_key:str):
        self.api_key = api_key
    
    def get_clusters_data(self, coordinates:list, n_minutes: int, date: str) -> Response :
        """
        Returns a response with clusters metadata data using the filters provided
        """
        url = "https://app.ororatech.com/v1/clusters/"
        header = {"apikey" : self.api_key}
        
        xmin, ymin, xmax, ymax = coordinates
        params = {
            "xmin": xmin, #lonmin
            "ymin": ymin, #latmin
            "xmax": xmax, #lonmax
            "ymax": ymax, #latmax
            "satellites":["NOAA-20", "SUOMI-NPP", "AQUA","TERRA", "SENTINEL-2A","SENTINEL-2B","LANDSAT-8","Himawari-8","Himawari-9"],
            "minutes": n_minutes,
            "date": date,
            "select": ["confidence","types","oldest_acquisition","newest_acquisition"]
        }
        
        # Retry the request 5 times if it fails
        max_retries = 5
        for i in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=header)
                if response.status_code == 200:
                    return response
                else:
                    raise RuntimeError(
                        "API request not successful, status code " + str(response.status_code) + " for date " + str(date)
                    )
            except RuntimeError as e:
                log.error(e)
                if i < max_retries - 1:
                    log.warning(f"Retrying {i+1} time")
                    time.sleep(5)
                else:
                    log.warning("Max retries reached! Could not get clusters data for the date")
                    return None
    
    def preprocess_clusters(self, data: Response, DATE: datetime.datetime) -> gpd.GeoDataFrame:
        """ Returns preprocessed clusters data which fullfills either the clusters confidence >= 0.6 or classified as fire
        """

        """
        Preprocessing steps include:
        - Filter the clusters that are active in future
        - Consider only clusters with consifdence >= 0.6 or classified as fire types
        - Add Date column to the gdf which stores the timestamp we queried the data for
        - Rename the columns
        """

        data = data.json()["features"]
        if not data:
            log.info(f"No clusters for {DATE}")
            return None
        gdf = gpd.GeoDataFrame.from_features(data)
        gdf["types"] = gdf["types"].astype(str)

        log.info(f"No. of Clusters before pre-processing for {DATE} are {len(gdf)}")

        # preprocess the clusters that are active in future 
        gdf["newest_acquisition"] = pd.to_datetime(gdf["newest_acquisition"], format="%Y-%m-%dT%H:%M:%SZ")
        condition_1 = gdf["newest_acquisition"] >= DATE
        gdf = gdf[condition_1]
        if gdf.empty:
            log.info(f"No clusters after  pre-prcoessing for {DATE} because of condition 1")
            return None
        # Consiering only the following types of fires Ref: https://gitlab.com/ororatech/groundsegment/wildfireservice/-/blob/develop/backend/assets/confirmation_types.json?ref_type=heads
        type_values = ["[1]","[2]", "[5]", "[6]", "[7]","[8]", "[13]"]
        condition_2 = gdf["types"].isin(type_values)
        condition_3 = gdf["confidence"] >= 0.6

        gdf_preprocessed = gdf[condition_2 | condition_3]
        if gdf_preprocessed.empty:
            log.info(f"No clusters after  pre-processing for {DATE} because of condition 2 and 3")
            return None
        
        gdf_preprocessed["date"] = pd.to_datetime(DATE, format="%Y-%m-%d-%H%M")
        gdf_preprocessed = gdf_preprocessed.rename(columns={"id":"cluster_id", "newest_acquisition":"cluster_newest_acquisition", "oldest_acquisition":"cluster_oldest_acquisition"})
        gdf_preprocessed = gdf_preprocessed[['date', 'cluster_id', 'num_fires', 'confidence', 'types','cluster_newest_acquisition','cluster_oldest_acquisition']]
        gdf_preprocessed['types'] = gdf_preprocessed['types'].str.strip('[]')
        gdf_preprocessed['types'] = gdf_preprocessed['types'].astype(int)   
        log.info(f"No. of Clusters after  pre-processing for {DATE} are {len(gdf_preprocessed)}")

        return gdf_preprocessed
    
    def get_fire_events_data(self, id: int) -> Response:
        """
        Returns a filtered dataframe of fire events
        """
        header = {"apikey" : self.api_key}
        endpoint = "https://app.ororatech.com/v1/clusters/"
        url = f"{endpoint}{id}" 

        params = {
            "select" : "events"
        }

         # Retry the request 5 times if it fails
        max_retries = 5
        for i in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=header)
                if response.status_code == 200:
                    return response
                else:
                    raise RuntimeError(
                        "API request not successful, status code " + str(response.status_code) +  " for cluster id " + str(id)
                    )
            except RuntimeError as e:
                log.error(e)
                if i < max_retries - 1:
                    log.warning(f"Retrying {i+1} time")
                    time.sleep(5)
                else:
                    log.warning("Max retries reached! Could not get clusters data for the date")
                    return None
    
    def preprocess_fire_events(self, data: Response, time: datetime.datetime, TIMEDELTA: int, cluster_id: int, cluster_oldest_time: str) -> gpd.GeoDataFrame:
            
            """ 
            Returns preprocessed fire events data
            """
            time = datetime.datetime.strptime(time, "%Y-%m-%d-%H%M")
            time = pd.Timestamp(time, tz=UTC)
            cluster_oldest_time = datetime.datetime.strptime(cluster_oldest_time, "%Y-%m-%d-%H%M")
            cluster_oldest_time = pd.Timestamp(cluster_oldest_time, tz=UTC)
    
            data = data.json()["properties"]["fire_events"]
            geometry = [Point(xy) for xy in zip([d['lon'] for d in data], [d['lat'] for d in data])]
            fire_events = gpd.GeoDataFrame(data, geometry=geometry, crs="EPSG:4326")

            if "frp" not in fire_events.columns:
                log.info(f"No fire events data after pre-processing for cluster_id {cluster_id} at {time} because no frp column found")
                return None
            
            length_before = len(fire_events)
            fire_events = fire_events[['id','gsd', 'acquisition_time','frp', 'product_id', 'satellite', 'algorithm', 'geometry']]
            fire_events.rename(columns={"id":"fire_event_id"}, inplace=True)
            fire_events["acquisition_time"] = pd.to_datetime(fire_events["acquisition_time"])

            # only collect the fire events that are detected (TIMEDELTA) minutes before the 'time'
            
            # if (time -cluster_oldest_time).total_seconds() // 60 < TIMEDELTA:
            #     if (time -cluster_oldest_time).total_seconds() // 60 < 40:
            #         TIMEDELTA = 40
            #     else:
            #         TIMEDELTA = (time -cluster_oldest_time).total_seconds() // 60
                
            fire_events_condition = (fire_events["acquisition_time"] >= time - datetime.timedelta(minutes=TIMEDELTA)) & (fire_events["acquisition_time"] <= time)
            fire_events = fire_events[fire_events_condition]
            fire_events["acquisition_time"] = fire_events["acquisition_time"].astype(str)

            if fire_events.empty:
                log.info(f"No fire events data after pre-processing for cluster_id {cluster_id} at {time}")
                return None
            
            fire_events["fire_cluster_id"] = cluster_id   
            log.info(f"Preprocessed fire events data for {time} has {len(fire_events)} fire events out of {length_before} for cluster id {cluster_id}")

            return fire_events

