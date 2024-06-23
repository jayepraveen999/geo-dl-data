from rasterio.warp import reproject, Resampling
import rasterio
import glob
import os
from tqdm.auto import tqdm  # provides a progressbar
import logging as log

WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/04_pre_processing/reproject_resample_landcover.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"

)

files_2020 = glob.glob("03_aux_data/land_cover/2020/*.tif")
files_2021 = glob.glob("03_aux_data/land_cover/2021/*.tif")
log.info(len(files_2020), len(files_2021))
# dst_crs = "+proj=geos +over +lon_0=140.700 +lat_0=0.000 +a=6378137.000 +f=0.0033528129638281333 +h=35785863.0"
empty_raster = rasterio.open('data/himawari8/empty_mask_h8_aoi_updated.tif')
dst_crs = empty_raster.crs
transform, width, height = empty_raster.transform, empty_raster.width, empty_raster.height

for file in tqdm(files_2021):
    if not os.path.exists(f"data/aux_data/land_cover/2021/reprojected_resampled/r_r_{file.split('/')[-1]}"):
        with rasterio.open(file) as src:
            # transform, width, height = calculate_default_transform(
            #     src.crs, dst_crs, src.width, src.height, *src.bounds)
        
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height
                })
            log.info(file)
            with rasterio.open(f"data/aux_data/land_cover/2021/reprojected_resampled/r_r_{file.split('/')[-1]}","w", **kwargs) as dst:
                for i in range(1,src.count+1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.mode)
                    
    else:
        log.info(f"File r_r_{file.split('/')[-1]} already exists")

log.info(f"Successfully reprojected and resampled 2021 files")

for file in tqdm(files_2020):
    if not os.path.exists(f"data/aux_data/land_cover/2020/reprojected_resampled/r_r_{file.split('/')[-1]}"):
        with rasterio.open(file) as src:
            # transform, width, height = calculate_default_transform(
            #     src.crs, dst_crs, src.width, src.height, *src.bounds)
        
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height
                })
            log.info(file)
            with rasterio.open(f"data/aux_data/2020/reprojected_resampled/r_r_{file.split('/')[-1]}","w", **kwargs) as dst:
                for i in range(1,src.count+1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.mode)
                    
    else:
        log.info(f"File r_r_{file.split('/')[-1]} already exists")

log.info(f"Successfully reprojected and resampled 2020 files")

