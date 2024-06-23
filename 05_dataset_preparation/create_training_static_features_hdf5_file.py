import h5py
from datetime import datetime, timedelta
import numpy as np
import rasterio
import glob
import yaml
import logging as log
import warnings


log_file = f"train_test_split_data_files/train_split/static_files/create_training_static_features_hdf5_file.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

warnings.filterwarnings("ignore")


RASTER_WINDOWS = [(1024, 256), (1280, 256), (1536, 256), (1792, 256), (2048, 256), (2304, 256), (2560, 256), (512, 512), (768, 512), (1024, 512), (1536, 512), (1792, 512), (2048, 512), (2304, 512), (2560, 512), (3072, 512), (3328, 512), (3584, 512), (256, 768), (512, 768), (768, 768), (1024, 768), (1280, 768), (1536, 768), (1792, 768), (2048, 768), (2304, 768), (2560, 768), (3072, 768), (3328, 768), (3584, 768), (3840, 768), (256, 1024), (512, 1024), (768, 1024), (1024, 1024), (1280, 1024), (1536, 1024), (1792, 1024), (2048, 1024), (2304, 1024), (2560, 1024), (2816, 1024), (3072, 1024), (3328, 1024), (3584, 1024), (3840, 1024), (256, 1280), (512, 1280), (768, 1280), (2048, 1280), (2304, 1280), (2560, 1280), (2816, 1280), (3072, 1280), (3328, 1280), (3584, 1280), (3840, 1280), (256, 1536), (512, 1536), (1792, 1536), (2048, 1536), (2304, 1536), (2560, 1536), (2816, 1536), (3072, 1536), (3328, 1536), (3584, 1536), (3840, 1536), (256, 1792), (512, 1792), (1536, 1792), (2048, 1792), (2304, 1792), (2560, 1792), (2816, 1792), (3072, 1792), (3328, 1792), (3584, 1792), (3840, 1792), (4096, 1792), (1280, 2048), (2048, 2048), (2304, 2048), (2560, 2048), (2816, 2048), (3072, 2048), (3328, 2048), (3584, 2048), (3840, 2048), (4096, 2048), (2048, 2304), (2304, 2304), (2560, 2304), (3072, 2304), (3328, 2304), (3584, 2304), (3840, 2304), (4352, 2304), (2304, 2560), (2560, 2560), (2816, 2560), (3328, 2560), (3584, 2560), (1792, 2816), (2560, 2816), (2816, 2816)]


landcover_2020_data = rasterio.open("reprocess_data_2/input_data/auxiliary_data/land_cover/2020/reprojected_resampled/2020_landcover_finalized.tif")
landcover_2021_data = rasterio.open("reprocess_data_2/input_data/auxiliary_data/land_cover/2021/reprojected_resampled/2021_landcover_finalized.tif")
copdem_data = rasterio.open("reprocess_data_2/input_data/auxiliary_data/copdem/reprojected_resampled/copdem_90m_finalized.tif")
biomes_data = rasterio.open("reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/reprojected_resampled/biomes_2017_finalized.tif")
fldk_data = "reprocess_data_2/input_data/himawari8/ten_minute/2020/01/01/0500/0430/2020_01_01_0500_0430_stacked_masked.tif"


lat_window_data = []
lon_window_data = []
with rasterio.open(fldk_data) as src:
     for window in RASTER_WINDOWS:
          tile = src.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))
          height = tile.shape[0]
          width = tile.shape[1]
          tile_window = rasterio.windows.Window(window[1], window[0], width, height)
          transform = src.window_transform(tile_window)
          cols, rows = np.meshgrid(np.arange(width), np.arange(height))
          xs, ys = rasterio.transform.xy(transform, rows, cols)
          lat_window_data.append(np.array(ys))
          lon_window_data.append(np.array(xs))

log.info("Lat and Lon data for the raster windows have been successfully extracted")

def get_landcover_window_data_and_water_fraction(landcover_data):
    
    landcover_window_data = []
    water_fraction = []
    for window in RASTER_WINDOWS:

        # read the raster using the window
        tile = landcover_data.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))
        landcover_window_data.append(tile)
        # calculate the water fraction
        water_fraction.append(np.sum(tile == 80) / (256 * 256))

    return landcover_window_data, water_fraction

landcover_2020_window_data, water_fraction_2020 = get_landcover_window_data_and_water_fraction(landcover_2020_data)
landcover_2021_window_data, water_fraction_2021 = get_landcover_window_data_and_water_fraction(landcover_2021_data)
log.info("Landcover data and water fraction for year 2020 and 2021 for the raster windows have been successfully extracted")

def get_biomes_copdem_window_data(biomes_data, copdem_data):
    biomes_window_data = []
    copdem_window_data = []
    for window in RASTER_WINDOWS:
        # read the raster using the window
        biomes_tile = biomes_data.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))
        copdem_tile = copdem_data.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))
        biomes_window_data.append(biomes_tile)
        copdem_window_data.append(copdem_tile)

    return biomes_window_data, copdem_window_data

biomes_window_data, copdem_window_data = get_biomes_copdem_window_data(biomes_data, copdem_data)
log.info("Biomes and Copdem data for the raster windows have been successfully extracted")


lats = np.array(lat_window_data, dtype='float32')
lons = np.array(lon_window_data, dtype='float32')
landcover_2020 = np.array(landcover_2020_window_data, dtype='int8')
landcover_2021 = np.array(landcover_2021_window_data, dtype='int8')
biomes = np.array(biomes_window_data, dtype='int8')
copdem = np.array(copdem_window_data, dtype='float32')
water_fraction_2020 = np.array(water_fraction_2020, dtype='float32')
water_fraction_2021 = np.array(water_fraction_2021, dtype='float32')

# print all the sizes of the arrays
log.info(f"lats shape: {lats.shape}")
log.info(f"lons shape: {lons.shape}")
log.info(f"landcover_2020 shape: {landcover_2020.shape}")
log.info(f"landcover_2021 shape: {landcover_2021.shape}")
log.info(f"biomes shape: {biomes.shape}")
log.info(f"copdem shape: {copdem.shape}")
log.info(f"water_fraction_2020 shape: {water_fraction_2020.shape}")
log.info(f"water_fraction_2021 shape: {water_fraction_2021.shape}")

log.info("All the static features have been successfully extracted and converted to numpy arrays")


with h5py.File('train_test_split_data_files/train_split/static_files/training_static_data.h5', 'w') as f:
    #  creata a static group for the input features and save datasets such as landcover_2020, landcover_2021, biomes, copdem, lat, lon

    # create a static group for the input features landcover_2020, landcover_2021, biomes, copdem, lat, lon and raster_windows
    static_group = f.create_group("static_features")
    static_group.create_dataset("landcover_2020", data=landcover_2020)
    static_group.create_dataset("landcover_2021", data=landcover_2021)
    static_group.create_dataset("water_fraction_2020", data=water_fraction_2020)
    static_group.create_dataset("water_fraction_2021", data=water_fraction_2021)
    static_group.create_dataset("biomes", data=biomes)
    static_group.create_dataset("copdem", data=copdem)
    static_group.create_dataset("lat", data=lats)
    static_group.create_dataset("lon", data=lons)
    static_group.create_dataset("raster_windows", data = np.array(RASTER_WINDOWS, dtype='int32'))
    # write the metadata for the static group
    static_group.attrs["description"] = "This group contains the static input features for the model where each dataset is multidimensional array with shape (107,256,256) which represents the data for 107 raster windows of size 256x256. You can find more information about the raster windows in the raster_windows dataset."

log.info("All the static features have been successfully saved to the hdf5 file")
            


