import numpy as np
import glob
import rasterio 
import json
import numpy as np
import logging as log
import os

WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/04_pre_processing/updated_no_data_labels_2022.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"

)

def main():
    pass

if __name__ == "__main__":

    # using locally saved unique timestamps 
    with open("data/fire_masks/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)

    

    for timestamp in timestamps:
        if timestamp.startswith("2022"):
        
            # get labels for the timestamp
            labels_file = glob.glob(f"data/himawari8/{timestamp}*_cmsk_applied_labels.tif")
            if len(labels_file) != 1:
                log.info(f"Something is wrong with labels raster for {timestamp}, len(labels_file) = {len(labels_file)}")
                continue
            else:
                with rasterio.open(labels_file[0]) as src:
                    labels_binary = src.read(1)
                    # labels_4 = src.read(2)
                    space_mask = src.read(3)
                    
                    # converting dtypes of array to float32
                    labels_binary = labels_binary.astype(np.float32)
                    # labels_4 = labels_4.astype(np.float32)
                    space_mask = space_mask.astype(np.float32)

                    # updating the labels
                    labels_binary[space_mask == 0.0] = np.nan
                    # labels_4[space_mask == 0.0] = np.nan

                    profile = src.profile
                    profile.update(dtype=rasterio.float32)
                    profile.update(count=1)

                    # writing the updated labels to the same file
                    with rasterio.open(f"data/himawari8/{timestamp}{timestamp.replace('/','_')}_cmsk_applied_labels_with_nan.tif", 'w', **profile) as dst:
                        dst.write(labels_binary, 1)

                        # not writing these two bands for now
                        # dst.write(labels_4, 2)
                        # dst.write(space_mask, 3)

                log.info(f"Updated data labels for {timestamp} with Nan for values outside study area for only binary cloud mask")

    log.info("Updated labels files for 2022 with Nan for values outside study area")
