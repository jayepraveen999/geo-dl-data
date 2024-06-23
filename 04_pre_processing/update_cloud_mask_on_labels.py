import rasterio
import numpy as np
import os
import glob
import json
import logging as log
log_file = f"data/himawari8/update_cloud_mask_on_labels_2022.txt"  # Path to the log file

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

            # get cloud mask for the timestamp
            cloud_mask_file = glob.glob(f"data/himawari8/{timestamp}{timestamp.split('/')[-2]}/cd_mask_*.tif")
            if len(cloud_mask_file) == 0:
                log.info(f"Missing cloud mask for {timestamp}")
                continue
            elif len(cloud_mask_file) > 0:
                # reading the binary cloud mask which is the second band
                cloud_mask_4 = rasterio.open(cloud_mask_file[0]).read(1)
                cloud_mask_2 = rasterio.open(cloud_mask_file[0]).read(2)

            # get labels for the timestamp
            labels_file = glob.glob(f"data/himawari8/{timestamp}*_finalized_labels.tif")
            if len(labels_file) == 0:
                log.info(f"Missing labels for {timestamp}")
                continue

            elif len(labels_file) > 0:

                # check if the labels are already processed
                if not os.path.exists(f"data/himawari8/{timestamp}/{timestamp.replace('/','_')}_cmsk_applied_labels.tif"):
                    # reading the labels
                    with rasterio.open(labels_file[0]) as src:
                        labels = src.read(1)
                        space_mask = src.read(2)
                        
                        # creating a copies of the labels to apply cloud mask
                        updated_labels_2_cmsk = labels.copy()
                        updated_labels_4_cmsk = labels.copy()


                        # binary cloud mask applied on labels
                        updated_labels_2_cmsk[cloud_mask_2 == 1] = 0
                        # in four category cloud mask, we only consider CLOUDY (3) category as the cloud mask
                        updated_labels_4_cmsk[cloud_mask_4 == 3] = 0

                        labels_meta = src.meta
                        labels_meta.update({"count": 3})


                    # writing the labels with cloud mask applied
                    with rasterio.open(f"data/himawari8/{timestamp}/{timestamp.replace('/','_')}_cmsk_applied_labels.tif", "w", **labels_meta) as dest:
                        dest.write(updated_labels_2_cmsk, indexes=1)
                        dest.write(updated_labels_4_cmsk, indexes=2)
                        dest.write(space_mask, indexes=3)
                        log.info(f"Successfully applied cloud mask to the labels for {timestamp}")

                else:
                    log.info(f"Cloud mask already applied to the labels for {timestamp}")

    log.info("Applied cloud mas on labels for 2022")

        
