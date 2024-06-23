import rasterio
import numpy as np
import shutil
import os
import glob
import json
import logging as log

WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/04_pre_processing/merge_ahi_nonahi_labels_2022.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

if __name__=="__main__":

    # using locally saved unique timestamps 
    with open("data/fire_masks/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)

    for timestamp in timestamps:
        if timestamp.startswith("2022"):
        
            # get labels for the timestamp
            labels_file = glob.glob(f"data/himawari8/{timestamp}*_non_ahi_labels.tif") + glob.glob(f"data/himawari8/{timestamp}*_updated_ahi_labels.tif")
            if len(labels_file) == 1:
                # found only one file and hence this is the finaliized label. Copy the same file and rename it to finalized
                shutil.copy(labels_file[0], f"data/himawari8/{timestamp}{timestamp.replace('/','_')}finalized_labels.tif")
                log.info(f"Found only one file for {timestamp} and hence copied the same file and renamed it to finalized")

            else:
                # found more than one file and hence need to merge the labels
                log.info(f"Found more than one file for {timestamp} and hence need to merge the labels")

            #  create in memory raster by reading the two raster files and applying union and then use it as src for below operation
                with rasterio.open(labels_file[0]) as src1:
                    with rasterio.open(labels_file[1]) as src2:
                        labels_1 = src1.read(1)
                        labels_2 = src2.read(1)

                        # space mask is same hence taking from any of the raster
                        space_mask = src1.read(2)

                        # update the labels_1 with labels_2
                        labels_1[labels_2 == 1] = 1

                        # final labels
                        labels = labels_1

                        # same profile
                        profile = src1.profile
            

                    # writing the updated labels
                    with rasterio.open(f"data/himawari8/{timestamp}{timestamp.replace('/','_')}finalized_labels.tif", 'w', **profile) as dst:
                        dst.write(labels, 1)
                        dst.write(space_mask, 2)


                log.info(f"Successfully merged the labels for {timestamp} and created finalized labels file")

    log.info("Merged labels files for 2022")

