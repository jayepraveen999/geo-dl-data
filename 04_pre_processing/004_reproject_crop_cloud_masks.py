import rasterio
import datetime
from rasterio.io import MemoryFile
import numpy as np
import geopandas as gpd
import xarray as xr
import json
import glob
import os
import logging as log

WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/04_pre_processing/reproject_crop_clouds.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def main(cloud_data, timestamp, child_timestamp):
    
    # Read the cloud data
    cd_fldk = cloud_data["CloudMask"].to_numpy()
    cd_fldk_binary = cloud_data["CloudMaskBinary"].to_numpy()
    # Create the multi-band TIFF in memory
    with MemoryFile() as memfile:
        with memfile.open(driver='GTiff', 
                        width=WIDTH, height=HEIGHT, count=COUNT, 
                        dtype=rasterio.uint8, 
                        crs=CRS, 
                        transform=TRANSFORM) as dest:
            dest.write(cd_fldk, indexes=1)
            dest.write(cd_fldk_binary, indexes=2)
            dest.write(FLDK_SPACE_MASK, indexes=3)

        # Mask the in-memory dataset with AOI
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

    # Write the masked raster to the final output file
    with rasterio.open(f"data/himawari8/{timestamp}{child_timestamp}/cd_mask_{timestamp.replace('/','_')}_{child_timestamp}.tif", "w", **masked_meta) as dest:
        dest.write(masked_raster)
    
    log.info(f"Succesfully created the cloud mask raster for {timestamp}{child_timestamp}")


if __name__ == "__main__":

    EMPTY_RASTER = rasterio.open('data/himawari8/sample_data_B05_20220101_004000.tif')
    WIDTH = EMPTY_RASTER.width
    HEIGHT = EMPTY_RASTER.height
    COUNT = 3
    TRANSFORM = EMPTY_RASTER.transform
    CRS = EMPTY_RASTER.crs
    FLDK_SPACE_MASK = EMPTY_RASTER.read(2)


    AOI_H8 = gpd.read_file("02_input_data/aoi_h8_updated.geojson")
    AOI_H8_GEOM = AOI_H8["geometry"]

    # using locally saved unique timestamps for seed 12 
    with open("data/fire_masks/unique_dates_ten_minute_finalized.json") as json_file:
        timestamps = json.load(json_file)
    
    # timestamps =  [datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d/%H%M/") for timestamp in timestamps]
    for timestamp in timestamps:
        file = glob.glob(f"data/himawari8/{timestamp}{timestamp.split('/')[-2]}/*.nc")
        if len(file) == 0:
            log.info(f"No cloud files found in the directory {timestamp}{timestamp.split('/')[-2]}")
            continue
        elif len(file) > 1:
            log.info(f"More than two cloud files found in the directory {timestamp}{timestamp.split('/')[-2]}")
            continue
        else:
            if not os.path.exists(f"data/himawari8/{timestamp}{timestamp.split('/')[-2]}/cd_mask_{timestamp.replace('/','_')}_{timestamp.split('/')[-2]}.tif"):
                log.info(f"Processing cloud file {file[0]}")
                cloud_data = xr.open_dataset(file[0])
                main(cloud_data, timestamp, timestamp.split('/')[-2])
            else:
                log.info(f"Cloud mask raster already exists for {timestamp}{timestamp.split('/')[-2]}")
                continue        