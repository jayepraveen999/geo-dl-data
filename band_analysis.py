import satpy
import numpy as np
import glob
import rasterio
import utils
import geopandas as gpd
from shapely.geometry import shape
import matplotlib.pyplot as plt
from shapely.ops import transform
from rasterio.mask import mask

def main():
    filenames = glob.glob('data/band_analysis_data/*.DAT')
    scene = satpy.Scene(reader='ahi_hsd', filenames=filenames)

    # scene.load(scene.available_dataset_names(), calibration='counts')
    bt_bands = ['B07', 'B08', 'B09', 'B10', 'B11', 'B12', 'B13', 'B14', 'B15', 'B16']
    scene.load(bt_bands, calibration='brightness_temperature')

    # save datasets as geotiffs   
    scene.save_datasets(writer='geotiff', base_dir='data/band_analysis_data/bt_geotiffs')


def band_analysis():

    # read the aoi
    # aoi_path = "data/band_analysis_data/response_1702221129143.geojson.json"
    aoi_path = "data/band_analysis_data/response_1702218842422.geojson.json"
    gdf = gpd.read_file(aoi_path)
    geometry = gdf['geometry'].iloc[0]

    # Convert the geometry to a Shapely geometry object
    aoi = shape(geometry)

    # custom transformer
    project = utils.get_h8_proj_transformer('EPSG:4326')
    aoi_geometry = transform(project, aoi)

    # read relavant raster bands data
    file_names = glob.glob("data/himawari8/band_analysis/bt_geotiffs/*.tif")
    file_names = sorted(file_names)

    # for band plots as images to see fire pixels
    num_files = len(file_names)
    num_cols = 3  # Number of columns for subplots
    num_rows = -(-num_files // num_cols)  # Calculate the number of rows needed

    plt.figure(figsize=(15, 5 * num_rows))

    # get the mean/sum of the brightness temperature for each band
    masked_band_names = []
    masked_sum = []
    for idx,i in enumerate(file_names):
        # write the first three letters of the band name
        band_name = i.split("/")[-1][:3]
        src = rasterio.open(i)
        masked_data, masked_transform = mask(src, [aoi_geometry], crop=True)
        masked_band_names.append(band_name)
        masked_sum.append(np.sum(masked_data[0]))
    #     plt.subplot(num_rows, num_cols, idx + 1)
    #     plt.imshow(masked_data[0], cmap='viridis')  # You can choose any colormap you prefer
    #     plt.title(f"Band: {band_name}")
    #     plt.colorbar()
    #     plt.axis('off')
    # plt.suptitle("Brightness Temperature for each band for cluster 14204743 at 2021/02/01/0630", y=1, fontsize=20)
    # plt.tight_layout()
    # plt.savefig(f"../data/band_analysis_data/band_analysis_band_wise_fire_plots_14204743_2021_02_01_0630.png")
    # plt.show()

    plt.figure(figsize=(10, 5))
    plt.bar(masked_band_names, masked_sum, color='skyblue', edgecolor='black')
    plt.xlabel("Band")
    plt.ylabel("Sum of BT values")
    plt.title("Sum of Brightness Temperature for each band for cluster 25006763")
    plt.savefig("dataset_analysis/band_analysis/band_analysis_bt_sum2.png")
    plt.show()
        
if __name__ == "__main__":
    # main()
    band_analysis()

