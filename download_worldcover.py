import os
import requests
import yaml
import geopandas as gpd
from tqdm.auto import tqdm  # provides a progressbar
from shapely.wkt import loads



# get AOI from CONFIG
# Load the configuration files
with open("config/fire_labels_auth.yml", 'r') as stream:
    try:
        CONFIG = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


WKT = CONFIG['AOI_WKT']
S3_URL_PREFIX = "https://esa-worldcover.s3.eu-central-1.amazonaws.com"
AOI = loads(WKT)

# load worldcover grid
url_2021 = f'{S3_URL_PREFIX}/v200/2021/esa_worldcover_grid.geojson'
url_2020 = f'{S3_URL_PREFIX}/v100/2020/esa_worldcover_2020_grid.geojson'
grid = gpd.read_file(url_2020)
print(len(grid))

# get grid tiles intersecting AOI
tiles = grid[grid.intersects(AOI)]
print(len(tiles))

# download worldcover data for each tile
for tile in tqdm(tiles.ll_tile):
    if not os.path.exists(f"reprocess_data/input_data/auxiliary_data/land_cover/2020/ESA_WorldCover_10m_2020_v100_{tile}_Map.tif"):
        url = f"{S3_URL_PREFIX}/v100/2020/map/ESA_WorldCover_10m_2020_v100_{tile}_Map.tif"
        r = requests.get(url, allow_redirects=True)
        out_fn = f"reprocess_data/input_data/auxiliary_data/land_cover/2020/ESA_WorldCover_10m_2020_v100_{tile}_Map.tif"
        with open(out_fn, 'wb') as f:
            f.write(r.content)
    else:
        print(f"File {tile} already exists")