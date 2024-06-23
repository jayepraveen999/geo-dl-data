import geopandas as gpd
from datetime import datetime
import os
import json
import logging as log

WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/01_fire_masks/filter_fire_labels.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def main(seed: int, minute: str):

    
    if os.path.exists(f"data/fire_masks/2020_2021_2022_combined_{seed}_{minute}_preprocessed.geojson"):
            log.info(f"data/fire_masks/2020_2021_2022_combined_{seed}_{minute}_preprocessed.geojson already exists")
            return 
    
    seed_data = gpd.read_file(f"reprocess_data_2/fire_labels/{minute}/2020_2021_2022_combined_{seed}.geojson")

    # remove satellites that we don't want
    preprocessed_data = seed_data[(seed_data["satellite"] != "GK2A") & (seed_data["satellite"] != "Meteosat-8") & (seed_data["satellite"] != "MetOp-B") & (seed_data["satellite"] != "MetOp-C") & (seed_data["satellite"] != "MetOp-A") & (seed_data["satellite"] != "LANDSAT-9") & (seed_data["satellite"] != "LANDSAT-8")]
    # remove labels which has OT-AI as algorithm
    preprocessed_data = preprocessed_data[preprocessed_data["algorithm"] != "OT-AI"]
    # plot a graph to see the trend of time from 0 to t by taking difference between acquition_time and cluster_oldest_acquisition for the dataframe
    preprocessed_data["time_diff"] = preprocessed_data["acquisition_time"] - preprocessed_data["cluster_oldest_acquisition"]    
    preprocessed_data["time_diff"] = preprocessed_data["time_diff"].dt.total_seconds()/60

    # only keep clusters that are relavant
    type_values = [0,1,5,6,7,8,13]
    finalized_data = preprocessed_data[preprocessed_data["types"].isin(type_values)]

    # remove clusters that are older than 2022/12/13/1720 as we don't have h8 data
    reference_date = datetime.strptime('2022/12/13/1720', '%Y/%m/%d/%H%M')
    finalized_data = finalized_data[finalized_data["date"] < reference_date]


    # check for unique dates from (365*4)*(3) = 4380 dates
    unique_dates = finalized_data["date"].nunique()
    log.info(f"Out of 4380 queried DATEs, we finally have {unique_dates} DATEs after preprcoessing")
    unique_dates_list = finalized_data["date"].dt.strftime("%Y/%m/%d/%H%M/").unique().tolist()
    file_path = f"data/fire_masks/unique_dates_{minute}.json"

    # Write the list to the JSON file
    with open(file_path, 'w') as json_file:
        json.dump(unique_dates_list, json_file)
        
    log.info(f"Total no of fire events after finalzing the labels are {len(finalized_data)}")
    finalized_data.to_file(f"data/fire_masks/2020_2021_2022_combined_{seed}_{minute}_preprocessed.geojson", driver="GeoJSON")

if __name__ == "__main__":

    # This script further cleans and finalizes the data for the seed
    seed = 1703273207
    ten_minute = 'ten_minute'

    main(seed, ten_minute)