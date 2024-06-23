from rasterio.warp import reproject, Resampling
import rasterio
import glob
import os
import logging as log


WORKDIR = os.getcwd()
log_file = f"{WORKDIR}/04_pre_processing/reproject_resample_copdem.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"

)

""" Clip the vrt using gdal_translate -projwin ulx uly lrx lry input.vrt output.tif --config AWS_NO_SIGN_REQUEST YES"""

file = glob.glob("03_aux_data/copdem/copdem_90m_clipped.tif")
log.info("Loaded Clipped COPDEM file")
# dst_crs = "+proj=geos +over +lon_0=140.700 +lat_0=0.000 +a=6378137.000 +f=0.0033528129638281333 +h=35785863.0"]empty_raster = rasterio.open('../data/input_data/himawari8/empty_mask_h8_aoi.tif')
empty_raster = rasterio.open('data/himawari8/empty_mask_h8_aoi_updated.tif')
log.info("Loaded empty raster")
dst_crs = empty_raster.crs
transform, width, height = empty_raster.transform, empty_raster.width, empty_raster.height


if not os.path.exists(f"data/aux_data/copdem/reprojected_resampled/r_r_{file[0].split('/')[-1]}"):
    with rasterio.open(file[0]) as src:
        # transform, width, height = calculate_default_transform(
        #     src.crs, dst_crs, src.width, src.height, *src.bounds)
    
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
            })
        with rasterio.open(f"data/aux_data/copdem/reprojected_resampled/r_r_{file[0].split('/')[-1]}","w", **kwargs) as dst:
            for i in range(1,src.count+1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear)
                
else:
    log.info(f"File r_r_{file[0].split('/')[-1]} already exists")

log.info(f"Successfully reprojected and resampled Clipped CopDEM file")


