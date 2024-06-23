import satpy
import glob
import os
import rasterio
import json
import numpy as np
import concurrent.futures
from utils import get_child_timestamps
from rasterio.mask import mask
from rasterio.io import MemoryFile
import geopandas as gpd
import logging as log
log_file = f"reprocess_data_2/input_data/himawari8/ten_minute/unzip_crop_stack_fldk_2022.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def unzip_covert_to_tiff(filenames, timestamp, child_timestamp):
    
    # check if the files are already unzipped and if yes, then skip
    len_files = len(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/B*.tif"))
    if len_files>0:
        log.info(f"files already unzipped for {timestamp}{child_timestamp.split('/')[-2]}, len_files: {len_files}")

        if len(filenames) != 0:
            # delete all .bz2 files
            log.info(f"Deleting .bz2 files for {timestamp}{child_timestamp.split('/')[-2]}")
            for filename in filenames:
                os.remove(filename)
        else:
            log.info(f"No .bz2 files to delete for {timestamp}{child_timestamp.split('/')[-2]}")
        return
    

    log.info(f"Unzipping for {timestamp}{child_timestamp.split('/')[-2]}")
    
    # unzip the files
    scene = satpy.Scene(reader='ahi_hsd', filenames=filenames)

    scene.load(scene.available_dataset_names(),calibration='brightness_temperature')
    log.info("loaded Scenes")

    # save datasets as geotiffs   
    scene.save_datasets(writer='geotiff', dtype= np.float32, enhance= False, base_dir=f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}")
    log.info("saved datasets")

    # after unzipping delete al .bz2 files
    log.info(f"Deleting .bz2 files for {timestamp}{child_timestamp.split('/')[-2]}")
    for filename in filenames:
        os.remove(filename)


def stack_bands_and_mask(files, timestamp, child_timestamp):

    # check if the files are already stacked and masked and if yes, then skip
    len_files = len(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/{timestamp.replace('/','_')}{child_timestamp.split('/')[-2]}_stacked_masked.tif"))
    if len_files>0:
        log.info(f"files already stacked and masked for {timestamp}{child_timestamp.split('/')[-2]}")
        return
    # Open all bands and stack them
    band_stack = [rasterio.open(band_path).read(1) for band_path in files]
    band_stack = np.stack(band_stack, axis=0)

    # Get metadata from one of the bands to use in the stacked file
    with rasterio.open(files[0]) as src:
        meta = src.meta.copy()
    
    # Update metadata for the stacked file
    meta.update({
        'count': band_stack.shape[0],  # Number of bands in the stacked file
        'dtype': np.float32  # Adjust dtype if needed
    })

    # Create an in-memory file to store the stacked bands
    with MemoryFile() as memfile:
        with memfile.open(**meta) as dst:
            dst.write(band_stack)

        # Apply mask to the stacked bands in memory
        with memfile.open() as src:
            masked_raster, masked_transform = rasterio.mask.mask(src, AOI_H8_GEOM, crop=True)
            masked_meta = src.meta.copy()

    # Update metadata for the masked raster
    masked_meta.update({
        "driver": "GTiff",
        "height": masked_raster.shape[1],
        "width": masked_raster.shape[2],
        "transform": masked_transform
    })

    # Save the masked raster as a new GeoTIFF file
    with rasterio.open(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/{timestamp.replace('/','_')}{child_timestamp.split('/')[-2]}_stacked_masked.tif", 'w', **masked_meta) as dst:
        dst.write(masked_raster)
    log.info(f"saved stacked and masked raster for timestamp: {timestamp}{child_timestamp.split('/')[-2]}")

def process_timestamp(timestamp):

    log.info(f"Processing timestamp: {timestamp}")
    # get the files for the timestamp
    child_timestamps = get_child_timestamps(timestamp)
    for child_timestamp in child_timestamps:
        filenames = sorted(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/*.bz2"))
        unzip_covert_to_tiff(filenames, timestamp, child_timestamp)
        tif_files = sorted(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/B*.tif"))
        stack_bands_and_mask(tif_files,timestamp,child_timestamp)


if __name__=="__main__":

    # load aoi
    AOI_H8 = gpd.read_file("reprocess_data_2/aoi_h8_updated.geojson")
    AOI_H8_GEOM = AOI_H8["geometry"]
    print(AOI_H8_GEOM)
    log.info("loaded AOI")
    
    # using locally saved unique timestamps 
    with open("reprocess_data_2/fire_labels/ten_minute/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)

    # remove timestamps for 2022 due to storage constraints | processing 2022 timestamps separately
    timestamps_2022 = []
    for timestamp in timestamps:
        if timestamp.split("/")[0] == "2022":
            timestamps_2022.append(timestamp)

    log.info(f"Total timestamps to be processed for 2022: {len(timestamps_2022)}")

    # Create a ThreadPoolExecutor with 23 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=23) as executor:
        # Submit tasks for each timestamp
        futures = [executor.submit(process_timestamp, timestamp) for timestamp in timestamps_2022]

        # Wait for all tasks to finish
        concurrent.futures.wait(futures)

    log.info("Done processing all timestamps")

    # for timestamp in timestamps_2022:

    #     log.info(f"Processing timestamp: {timestamp}")
    #     # get the files for the timestamp
    #     child_timestamps = get_child_timestamps(timestamp)
    #     for child_timestamp in child_timestamps:
    #         filenames = sorted(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/*.bz2"))
    #         unzip_covert_to_tiff(filenames, timestamp, child_timestamp)
    #         tif_files = sorted(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{child_timestamp.split('/')[-2]}/B*.tif"))
    #         stack_bands_and_mask(tif_files,timestamp,child_timestamp)
    #         break
    #     break

