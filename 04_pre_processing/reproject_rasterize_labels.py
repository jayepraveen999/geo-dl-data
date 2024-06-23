import os
import rasterio
import logging as log
import numpy as np
import geopandas as gpd
import rasterio.mask
from shapely.geometry import shape
from rasterio.features import rasterize
from utils import get_h8_proj4_string

log_file = "rasterize_labels.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
def create_empty_h8_mask():
    """
    Create an empty raster with the same extent as the aoi 
    """
    if not os.path.exists("reprocess_data_2/input_data/himawari8/empty_mask_h8_aoi_updated.tif"):
        # read aoi
        aoi_h8 = gpd.read_file("02_input_data/aoi_h8_updated.geojson")


        # mask the raster with aoi extent: take a sample file from himawari8
        with rasterio.open('reprocess_data_2/input_data/himawari8/sample_data_B05_20220101_004000.tif') as src:
            masked_raster, masked_transform = rasterio.mask.mask(src, aoi_h8["geometry"], crop=True)
            masked_meta = src.meta

        masked_meta.update({"driver": "GTiff",
                        "height": masked_raster.shape[1],
                        "width": masked_raster.shape[2],
                        "transform": masked_transform})

        # create an empty raster similar to masked raster
        masked_empty_raster = np.zeros_like(masked_raster)
        masked_empty_raster[1,:,:] = masked_raster[1,:,:]

        with rasterio.open("reprocess_data_2/input_data/himawari8/empty_mask_h8_aoi_updated.tif", "w", **masked_meta) as dest:
            dest.write(masked_empty_raster)

        return
    else:
        log.warning("Empty raster already exists")
    return


if __name__ == "__main__":

    # create empty raster for reference
    create_empty_h8_mask()

    # read and rasterize labels
    labels = gpd.read_file("data/fire_masks/2020_2021_2022_combined_1703273207_ten_minute_preprocessed_finalized.geojson")
    log.info("Read labels")

    reprojected_labels = labels.to_crs(get_h8_proj4_string())
    log.info("Reprojected labels")

    non_ahi_data = reprojected_labels[reprojected_labels["algorithm"]!= "GA-AHI-SRSS"]
    ahi_data = reprojected_labels[reprojected_labels["algorithm"] == "GA-AHI-SRSS"]
    non_ahi_data_unique_dates = non_ahi_data["date"].unique()
    ahi_data_unique_dates = ahi_data["date"].unique()
    log.info(f"Separated AHI and NON-AHI labels")

    EMPTY_RASTER = rasterio.open('reprocess_data_2/input_data/himawari8/empty_mask_h8_aoi_updated.tif')
    WIDTH = EMPTY_RASTER.width
    HEIGHT = EMPTY_RASTER.height
    COUNT = EMPTY_RASTER.count
    TRANSFORM = EMPTY_RASTER.transform
    CRS = EMPTY_RASTER.crs

    log.info(f"Rasterizing NON-AHI labels")
    for date in non_ahi_data_unique_dates:
        date_labels = non_ahi_data[non_ahi_data["date"]==date]
        date_fires = [shape(geometry) for geometry in date_labels["geometry"]]
        date_str = date.strftime("%Y/%m/%d/%H%M")
        date_lbl = date.strftime("%Y_%m_%d_%H%M")
        log.info(f"Rasterizing {date_str}")
        # rasterize the labels
        if not os.path.exists(f"data/himawari8/{date_str}/{date_lbl}_non_ahi_labels.tif"):
            try:
                with rasterio.open(f"data/himawari8/{date_str}/{date_lbl}_non_ahi_labels.tif", "w", driver='GTiff', 
                        width=WIDTH, height=HEIGHT, count=COUNT, 
                        dtype=rasterio.uint8, 
                        crs=CRS, 
                        transform=TRANSFORM) as dest:
                    rasterized = rasterize(
                        shapes=date_fires,
                        out_shape=(HEIGHT, WIDTH),
                        fill=0,
                        transform=TRANSFORM,
                        default_value=1
                    
                    )
                    dest.write(rasterized,indexes=1)
                    dest.write(EMPTY_RASTER.read(2),indexes=2)
                log.info(f"Rasterized {date_str}")
            except Exception as e:
                log.error(f"Error rasterizing {date_str}: {e}")
                continue
        else:
            log.warning(f"Raster {date_str} already exists")

    
    log.info(f"Rasterizing AHI labels")
    for date in ahi_data_unique_dates:
        date_labels = ahi_data[ahi_data["date"]==date]
        date_fires = [shape(geometry) for geometry in date_labels["geometry"]]
        date_str = date.strftime("%Y/%m/%d/%H%M")
        date_lbl = date.strftime("%Y_%m_%d_%H%M")
        log.info(f"Rasterizing {date_str}")
        # rasterize the labels
        if not os.path.exists(f"data/himawari8/{date_str}/{date_lbl}_ahi_labels.tif"):
            try:
                with rasterio.open(f"data/himawari8/{date_str}/{date_lbl}_ahi_labels.tif", "w", driver='GTiff', 
                        width=WIDTH, height=HEIGHT, count=COUNT, 
                        dtype=rasterio.uint8, 
                        crs=CRS, 
                        transform=TRANSFORM) as dest:
                    rasterized = rasterize(
                        shapes=date_fires,
                        out_shape=(HEIGHT, WIDTH),
                        fill=0,
                        transform=TRANSFORM,
                        default_value=1
                    
                    )
                    dest.write(rasterized,indexes=1)
                    dest.write(EMPTY_RASTER.read(2),indexes=2)
                log.info(f"Rasterized {date_str}")
            except Exception as e:
                log.error(f"Error rasterizing {date_str}: {e}")
                continue
        else:
            log.warning(f"Raster {date_str} already exists")


