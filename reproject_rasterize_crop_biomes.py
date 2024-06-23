import geopandas as gpd
from shapely.wkt import loads
import rasterio
import numpy as np
from rasterio import features
import yaml
import os
from utils import get_h8_proj4_string
# get AOI from CONFIG
# Load the configuration files
with open("config/fire_labels_auth.yml", 'r') as stream:
    try:
        CONFIG = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def clip_biomes():
    
    if not os.path.exists("reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/biomes_2017_aoi.geojson"):
        print("Clipping biomes to AOI")
        WKT = CONFIG['AOI_WKT']
        # read full biomes shapefile
        biomes = gpd.read_file("reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/Ecoregions2017.shp")

        # clip biomes to AOI
        polygon = loads(WKT)
        aoi_gdf = gpd.GeoDataFrame(geometry=[polygon], crs=biomes.crs)
        biomes_aoi = gpd.clip(biomes, aoi_gdf)

        # save clipped biomes
        biomes_aoi.to_file("reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/biomes_2017_aoi.geojson", driver='GeoJSON')
    else:
        print(" Clipped file already exists")
        biomes_aoi = gpd.read_file("reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/biomes_2017_aoi.geojson")

    return biomes_aoi

if __name__ == '__main__':

    """Clip, Reproject and Rasterize biomes shapefile to GeosH8 projection""" 

    # clip biomes to AOI
    biomes_aoi = clip_biomes()
    # reproject to GeosH8
    biomes_aoi_geosh8 = biomes_aoi.to_crs(get_h8_proj4_string())

    # rasterize biomes
    EMPTY_RASTER = rasterio.open("reprocess_data_2/input_data/himawari8/empty_mask_h8_aoi_updated.tif")
    WIDTH = EMPTY_RASTER.width
    HEIGHT = EMPTY_RASTER.height
    COUNT = EMPTY_RASTER.count
    TRANSFORM = EMPTY_RASTER.transform
    CRS = EMPTY_RASTER.crs

    raster_array = np.zeros((HEIGHT, WIDTH), dtype=np.uint8)

    with rasterio.open(f"reprocess_data_2/input_data/auxiliary_data/biomes/biomes_2017/biomes_2017_aoi_reprojected_rasterized.tif", "w", driver='GTiff', 
            width=WIDTH, height=HEIGHT, count=COUNT, 
            dtype=rasterio.uint8, 
            crs=CRS, 
            transform=TRANSFORM) as dest:
        for idx, rows in biomes_aoi_geosh8.iterrows():
            geom = rows["geometry"]
            value = rows["BIOME_NUM"]
            shapes = [(geom, value)]
            features.rasterize(shapes=shapes, fill=value, out=raster_array, transform=TRANSFORM)

        # used as biome mask
        dest.write(raster_array,indexes=1)
        # used as space mask
        dest.write(EMPTY_RASTER.read(2),indexes=2)

    print("Successfully clipped, reprojected and rasterized biomes shapefile")