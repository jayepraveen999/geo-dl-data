import geopandas as gpd
import pandas as pd
import numpy as np
import pathlib
import os
import json
import glob
import shutil
import h5py
import rasterio
import rasterio.mask
from datetime import datetime
from shapely.geometry import shape, MultiPolygon
from rasterio.features import rasterize, geometry_mask
from utils import get_h8_proj4_string, get_2022_timestamps
from pre_processing.reproject_rasterize_labels import create_empty_h8_mask


def create_input_ahi_cmsk_h8_fp_data(window, fire: gpd.GeoDataFrame):
    # create the input_ahi_data with band 0,4 for the fire using the timestamps
    # Convert start and end date strings to datetime objects
    base_dir = "reprocess_data_2/input_data/himawari8/ten_minute/"
    start_date_obj = datetime.strptime(fire["ignition_date"], "%Y/%m/%d/%H%M/")
    end_date_obj = datetime.strptime(fire["extinguish_date"], "%Y/%m/%d/%H%M/")

    # Filter dates falling within the specified range
    available_dates = [date for date in TIMESTAMPS_2022 if start_date_obj <= datetime.strptime(date, "%Y/%m/%d/%H%M/") <= end_date_obj]
    # create three empty arrays for ahi_date, cmsk_date, and h8_fire_product with shape (len(available_dates), 256, 256)
    ahi_data = np.zeros((len(available_dates), 6, 256, 256), dtype=np.float32)
    cloud_mask_binary = np.zeros((len(available_dates), 256, 256),  dtype=np.int8)
    h8_fire_product_data = np.zeros((len(available_dates), 256, 256),  dtype=np.int8)

    # now iterat through the available dates and create the ahi_data, cmsk_data, and h8_fire_product_data for the window
    for i, date in enumerate(available_dates):
        # read the ahi data
        stacked_masked = glob.glob(f"{base_dir}{date}{date.split('/')[-2]}/*_stacked_masked.tif")
        if len(stacked_masked) == 0 or len(stacked_masked) > 1:
            # raise an error
            raise ValueError(f"Error reading stacked_masked for date {date}")
        stacked_masked_file = rasterio.open(stacked_masked[0])
        ahi_data[i] = stacked_masked_file.read(window=rasterio.windows.Window(window[1], window[0], 256, 256))

        # read the cmsk data
        cmsk_binary = glob.glob(f"{base_dir}{date}{date.split('/')[-2]}/cd_mask_*.tif")
        if len(cmsk_binary) == 0 or len(cmsk_binary) > 1:
            # raise an error
            raise ValueError(f"Error reading cd_mask for date {date}")
        cmsk_binary_file = rasterio.open(cmsk_binary[0])
        cloud_mask_binary[i] = cmsk_binary_file.read(2, window=rasterio.windows.Window(window[1], window[0], 256, 256))

        # read the h8 fire product data
        h8_fire_product = glob.glob(f"{base_dir}{date}/*_{date.split('/')[-2]}_ahi_labels.tif")
        if len(h8_fire_product) == 0:
            # np ahi labels for this date hence fill the window with zeros
            h8_fire_product_data[i] = np.zeros((256, 256), dtype=np.int8)
        else:
            h8_fire_product_file = rasterio.open(h8_fire_product[0])
            h8_fire_product_data[i] = h8_fire_product_file.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))

    return ahi_data, cloud_mask_binary, h8_fire_product_data, available_dates
    


def create_h5py(bushfire_gdf: gpd.GeoDataFrame, h5py_path: str):

    ob_ids = bushfire_gdf["OBJECTID"].unique().tolist()
    
    with h5py.File(h5py_path, 'a') as f:

        fire_id_lists = []
        raster_window_id_lists = []
        fire_type_lists = []
        ignition_date_lists = []
        extinguish_date_lists = []
        area_ha_lists = []
        fire_life_lists = []
        bushfire_label_data_lists = []
        ahi_data_lists = []
        cloud_mask_binary_lists = []
        h8_fire_product_data_lists = []
        timestamps_lists = []
        timestamps_index_lists = []

        for ob_id in ob_ids:

            ''' the logic should be as follows:
            
            - first rasterize the label data using the current setup
            - retrieve the raster windpw ids for the fire. If more than one id is found, we create subgroups for each raster window id
            - look for other labels if they fall within the raster window and fire duration and update the labels for the corresponding raster window.
            - fetch all available ahi data that is already downloaded for these windows and create the input_ahi_data
            - create h8 fire product labels for these input data. if not available for a particular timestamp, fill the window with zeros

            '''
            # create a tmp folder where we create some rasters and delete them after the loop
            pathlib.Path(f"tmp/{ob_id}").mkdir(parents=True, exist_ok=True)
            
            fire = bushfire_gdf[bushfire_gdf["OBJECTID"] == ob_id].iloc[0]
            # change the date time format to %Y/%m/%d/%H%M/
            fire["ignition_date"] = fire["ignition_date"].strftime("%Y/%m/%d/%H%M/")
            fire["extinguish_date"] = fire["extinguish_date"].strftime("%Y/%m/%d/%H%M/")
            fire_geometries = [geometry for geometry in fire["geometry"].geoms]

            # create a rasteer for the current fire
            if not os.path.exists(f"tmp/{ob_id}/current_fire.tif"):
                try:
                    with rasterio.open(f"tmp/{ob_id}/current_fire.tif", "w", driver='GTiff', 
                                    width=WIDTH, height=HEIGHT, count=COUNT, 
                                    dtype=rasterio.uint8, 
                                    crs=CRS, 
                                    transform=TRANSFORM) as dest:
                        rasterized = geometry_mask(fire_geometries, out_shape=(HEIGHT, WIDTH), transform=TRANSFORM, all_touched=True, invert=True)
                        if np.sum(rasterized.astype(rasterio.uint8)) <= 0:
                            print(f"Fire {ob_id} has no rasterized data even though there are geometries")
                            continue
                        dest.write(rasterized.astype(rasterio.uint8), indexes=1)

                    # print(f"Rasterized current fire{ob_id}")
                except Exception as e:
                    print(f"Error rasterizing current fire{ob_id}: {e}")
                    continue
            
            # get all the overlapping fires for the current fire
            overlapping_fires = bushfire_gdf[(bushfire_gdf['ignition_date'] <= fire['extinguish_date']) & (bushfire_gdf['extinguish_date'] >= fire['ignition_date'])]
            # check if the current fire is in the overlapping fires
            if len(overlapping_fires[overlapping_fires["OBJECTID"]==ob_id]) > 0:
                overlapping_fires = overlapping_fires[overlapping_fires["OBJECTID"] != ob_id]
            
            # print(f"Number of overlapping fires for current fire {ob_id} are {len(overlapping_fires)} ")

            # overlapping_geometries = [geometry for geometry in overlapping_fires["geometry"].geoms]
            overlapping_geometries = []
            for geometry in overlapping_fires["geometry"]:
                # If the geometry is a MultiPolygon, access its constituent polygons
                if geometry.geom_type == "MultiPolygon":
                    for geom in geometry.geoms:
                        overlapping_geometries.append(geom)
                else:
                    # If it's a single Polygon, append it directly
                    overlapping_geometries.append(geometry)

            # create a raster for overlapping fires
            if not os.path.exists(f"tmp/{ob_id}/overlapping_fires.tif"):
                try:
                    with rasterio.open(f"tmp/{ob_id}/overlapping_fires.tif", "w", driver='GTiff', 
                                    width=WIDTH, height=HEIGHT, count=COUNT, 
                                    dtype=rasterio.uint8, 
                                    crs=CRS, 
                                    transform=TRANSFORM) as dest:
                        rasterized = geometry_mask(overlapping_geometries, out_shape=(HEIGHT, WIDTH), transform=TRANSFORM, all_touched=True, invert=True)
                        dest.write(rasterized.astype(rasterio.uint8), indexes=1)
                    # print(f"Rasterized overlapping fires {ob_id}")
                except Exception as e:
                    print(f"Error rasterizing overlapping fires{ob_id}: {e}")
                    continue

            fire_data = rasterio.open(f"tmp/{ob_id}/current_fire.tif")
            overlapping_fires_data = rasterio.open(f"tmp/{ob_id}/overlapping_fires.tif")


            for window_id,window in enumerate(RASTER_WINDOWS):
                window_data = fire_data.read(window=rasterio.windows.Window(window[1], window[0], 256, 256))
                # count if the window sum is greater than 0 then we have the fire in the window
                if np.sum(window_data) > 0:
                    # go to the same window in overlapping fires and update the window_data with the overlapping fires
                    overlapping_window_data = overlapping_fires_data.read(window=rasterio.windows.Window(window[1], window[0], 256, 256))
                    window_data[overlapping_window_data == 1] = 1
                    ahi_data, cloud_mask_binary, h8_fire_product_data, available_dates =  create_input_ahi_cmsk_h8_fp_data(window, fire)

                    #now make the other arrays size equal to the available_dates. Repetitive but we want to have a flat data structure
                    fire_id = np.full((len(available_dates), 1), ob_id, dtype=np.int32)
                    raster_window_id =  np.full((len(available_dates), 1), window_id, dtype=np.int32)
                    fire_type = np.full((len(available_dates), 1), fire["fire_type"], dtype='S16')
                    ignition_date = np.full((len(available_dates), 1), fire["ignition_date"], dtype='S16')
                    extinguish_date = np.full((len(available_dates), 1), fire["extinguish_date"], dtype='S16')
                    area_ha = np.full((len(available_dates), 1), fire["area_ha"], dtype=np.float32)
                    fire_life = np.full((len(available_dates), 1), fire["fire_life"], dtype='S')
                    bushfire_label_data = np.full((len(available_dates), 256, 256), window_data, dtype=np.uint8)
                    timestamps = np.array(available_dates, dtype='S').reshape(-1,1)
                    timestamps_index = np.arange(len(available_dates), dtype=np.int16).reshape(-1,1)

                    fire_id_lists.append(fire_id)
                    raster_window_id_lists.append(raster_window_id)
                    fire_type_lists.append(fire_type)
                    ignition_date_lists.append(ignition_date)
                    extinguish_date_lists.append(extinguish_date)
                    area_ha_lists.append(area_ha)
                    fire_life_lists.append(fire_life)
                    bushfire_label_data_lists.append(bushfire_label_data)
                    ahi_data_lists.append(ahi_data)
                    cloud_mask_binary_lists.append(cloud_mask_binary)
                    h8_fire_product_data_lists.append(h8_fire_product_data)
                    timestamps_lists.append(timestamps)
                    timestamps_index_lists.append(timestamps_index)


                    print(f"Added data for fire {ob_id} in window {window[0]}_{window[1]}")

            # remove the tmp folder
            # shutil.rmtree(f"tmp/{ob_id}")
            
            # break

        # concatenate all the lists and create the datasets
        f.create_dataset("fire_id", data=np.concatenate(fire_id_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("raster_window_id", data=np.concatenate(raster_window_id_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("fire_type", data=np.concatenate(fire_type_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("ignition_date", data=np.concatenate(ignition_date_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("extinguish_date", data=np.concatenate(extinguish_date_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("area_ha", data=np.concatenate(area_ha_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("fire_life", data=np.concatenate(fire_life_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("bushfire_label_data", data=np.concatenate(bushfire_label_data_lists, axis=0), chunks=(1,256,256), compression="lzf")
        f.create_dataset("ahi_data", data=np.concatenate(ahi_data_lists, axis=0), chunks=(1,6,256,256), compression="lzf")
        f.create_dataset("cloud_mask_binary", data=np.concatenate(cloud_mask_binary_lists, axis=0), chunks=(1,256,256), compression="lzf")
        f.create_dataset("h8_fire_product_data", data=np.concatenate(h8_fire_product_data_lists, axis=0), chunks=(1,256,256), compression="lzf")
        f.create_dataset("timestamps", data=np.concatenate(timestamps_lists, axis=0), chunks=(1,1), compression="lzf")
        f.create_dataset("timestamps_index", data=np.concatenate(timestamps_index_lists, axis=0), chunks=(1,1), compression="lzf")


if __name__ == '__main__':

    EVALUATION_DATASET_PATH = '/home/jayendra/thesis/geo-dl-fire-detection/reprocess_data_2/evaluation_data/bushfires_gad_preprocessed_2022.geojson'
    EVALUATION_H5PY_PATH = '/home/jayendra/thesis/geo-dl-fire-detection-model/data/evaluation_data/bushfires_gad_preprocessed_flat_2022.h5'
    data = gpd.read_file(EVALUATION_DATASET_PATH)
    FILTER_BUSHFIRES_ONLY = False
    DO_NOT_CONSIDER_LONG_AMBIGUOUS_BURNING_FIRE = False


    create_empty_h8_mask()
    EMPTY_RASTER = rasterio.open('reprocess_data_2/input_data/himawari8/empty_mask_h8_aoi_updated.tif')
    WIDTH = EMPTY_RASTER.width
    HEIGHT = EMPTY_RASTER.height
    COUNT = 1
    TRANSFORM = EMPTY_RASTER.transform
    CRS = EMPTY_RASTER.crs

    RASTER_WINDOWS = [(1024, 256), (1280, 256), (1536, 256), (1792, 256), (2048, 256), (2304, 256), (2560, 256), (512, 512), (768, 512), (1024, 512), (1536, 512), (1792, 512), (2048, 512), (2304, 512), (2560, 512), (3072, 512), (3328, 512), (3584, 512), (256, 768), (512, 768), (768, 768), (1024, 768), (1280, 768), (1536, 768), (1792, 768), (2048, 768), (2304, 768), (2560, 768), (3072, 768), (3328, 768), (3584, 768), (3840, 768), (256, 1024), (512, 1024), (768, 1024), (1024, 1024), (1280, 1024), (1536, 1024), (1792, 1024), (2048, 1024), (2304, 1024), (2560, 1024), (2816, 1024), (3072, 1024), (3328, 1024), (3584, 1024), (3840, 1024), (256, 1280), (512, 1280), (768, 1280), (2048, 1280), (2304, 1280), (2560, 1280), (2816, 1280), (3072, 1280), (3328, 1280), (3584, 1280), (3840, 1280), (256, 1536), (512, 1536), (1792, 1536), (2048, 1536), (2304, 1536), (2560, 1536), (2816, 1536), (3072, 1536), (3328, 1536), (3584, 1536), (3840, 1536), (256, 1792), (512, 1792), (1536, 1792), (2048, 1792), (2304, 1792), (2560, 1792), (2816, 1792), (3072, 1792), (3328, 1792), (3584, 1792), (3840, 1792), (4096, 1792), (1280, 2048), (2048, 2048), (2304, 2048), (2560, 2048), (2816, 2048), (3072, 2048), (3328, 2048), (3584, 2048), (3840, 2048), (4096, 2048), (2048, 2304), (2304, 2304), (2560, 2304), (3072, 2304), (3328, 2304), (3584, 2304), (3840, 2304), (4352, 2304), (2304, 2560), (2560, 2560), (2816, 2560), (3328, 2560), (3584, 2560), (1792, 2816), (2560, 2816), (2816, 2816)]

    # delete the .h5 file
    if os.path.exists(EVALUATION_H5PY_PATH):
        os.remove(EVALUATION_H5PY_PATH)

    TIMESTAMPS_2022 = get_2022_timestamps()

    # convert the ignition_date,  extinguish_datecolumn to datetime
    data["ignition_date"] = pd.to_datetime(data["ignition_date"], format="ISO8601")
    data["extinguish_date"] = pd.to_datetime(data["extinguish_date"], format="ISO8601")
    print(f"Number of records of bushire dataset for year 2022: {len(data)}")

    # filter records where extinguish_date is lesser than 2022-12-10 as we don't have h8 data
    data = data[data["extinguish_date"] < "2022-12-10"]
    print(f"Number of records after filtering on exitinguish date < 2022-12-10: {len(data)}")

    # create a new column called fire_life
    data["fire_life"] = data["extinguish_date"] - data["ignition_date"]

    #remove all the records where the fire life is less than 0
    data = data[data["fire_life"] > pd.Timedelta(0)]
    print(f"Number of records after filtering on fire_life > 0: {len(data)}")

    # remove all the records where area_ha is <=0
    data = data[data["area_ha"] > 0]
    print(f"Number of records after filtering on area_ha > 0: {len(data)}")

    if DO_NOT_CONSIDER_LONG_AMBIGUOUS_BURNING_FIRE:
        data = data[data["OBJECTID"] != 225136]
        print(f"Number of records after filtering on OBJECTID != 225136 because it burned for an year with quite less of an area: {len(data)}")

    if FILTER_BUSHFIRES_ONLY:
        # filter only bushfires
        data = data[data["fire_type"] == "Bushfire"]
        print(f"Number of records after filtering on fire_type == Bushfire: {len(data)}")

    # check if the objectid is unique
    assert data["OBJECTID"].is_unique, "objectid is not unique"

    # reproject the data to h8 projection
    data = data.to_crs(get_h8_proj4_string())
    print(f"Reprojected the data to H8 projection")

    # convert all polygons to multipolygon
    data["geometry"] = data["geometry"].apply(lambda x: MultiPolygon([x]) if x.geom_type == "Polygon" else x)

    # for every fire in data, create a group in h5py file
    create_h5py(data, EVALUATION_H5PY_PATH)
