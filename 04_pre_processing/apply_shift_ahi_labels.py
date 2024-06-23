import os
import glob
import rasterio
import numpy as np
import geopandas as gpd
from scipy.ndimage import label, generate_binary_structure
from scipy.ndimage import label, generate_binary_structure, find_objects
from scipy.signal import correlate2d
import logging as log
log_file = f"reprocess_data_2/input_data/himawari8/ten_minute/apply_shift_ahi_labels_2022.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def calculate_shift(fldk_slice,labels_slice)-> np.array:
    """
    Calculate the shift between two arrays using cross-correlation
    """
    # Compute cross-correlation
    cross_corr = correlate2d(fldk_slice, labels_slice)

    # Find the peak
    y_shift, x_shift = np.unravel_index(np.argmax(cross_corr), cross_corr.shape)

    # Calculate the shift relative to the center
    y_shift -= fldk_slice.shape[0] - 1
    x_shift -= fldk_slice.shape[1] - 1

    # Apply the shift to array2
    shifted_array2 = np.roll(np.roll(labels_slice, y_shift, axis=0), x_shift, axis=1)

    return shifted_array2


if __name__ == "__main__":

    # read labels
    labels_data = gpd.read_file("reprocess_data_2/fire_labels/ten_minute/2020_2021_2022_combined_1703273207_ten_minute_preprocessed_finalized.geojson")

    # read ahi-labels
    ahi_data = labels_data[labels_data["algorithm"] == "GA-AHI-SRSS"]
    ahi_data_dates = ahi_data["date"].unique()
    log.info("Read ahi labels")

    # as profile and background is same for all rasters, we will use the first raster as sample to get the profile and background
    ahi_labels_background = rasterio.open("reprocess_data_2/input_data/himawari8/ten_minute/2020/01/01/0500/2020_01_01_0500_non_ahi_labels.tif").read(2)
    ahi_labels_profile = rasterio.open("reprocess_data_2/input_data/himawari8/ten_minute/2020/01/01/0500/2020_01_01_0500_non_ahi_labels.tif").profile 

    # consider the diagonal pixels as well for label clustering using binary structure below
    s = generate_binary_structure(2,2)

    # for each date update the raster by the computed shift
    for date in ahi_data_dates:
        # dont process any date in 2022 | process 2022 dataset
        if date.year == 2022:

            # check if the date is already processed
            if os.path.exists(f"reprocess_data_2/input_data/himawari8/ten_minute/{date.strftime('%Y/%m/%d/%H%M')}/{date.strftime('%Y_%m_%d_%H%M')}_updated_ahi_labels.tif"):
                log.info(f"Already created updated labels for {date.strftime('%Y/%m/%d/%H%M')}")
                continue

            date_str = date.strftime("%Y/%m/%d/%H%M")
            date_lbl = date.strftime("%Y_%m_%d_%H%M")

            # read labels data
            ahi_labels_raster_data = rasterio.open(f"reprocess_data_2/input_data/himawari8/ten_minute/{date_str}/{date_lbl}_ahi_labels.tif").read(1)

            # read corresponding B07 data
            ahi_b7_raster_data = rasterio.open(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{date_str}/{date_str.split('/')[-1]}/*_masked.tif")[0]).read(1)


            # generate different numbers for the labels that are different in the same raster
            labeled_array, num_features = label(ahi_labels_raster_data, structure=s)
            log.info(f"Number of features for date {date} are: {num_features}")


            # Get bounding box indices for each labeled feature and update the labels with the shift calculated
            for feature in range(1,num_features+1):
                labeled_feature_indices = find_objects(labeled_array == feature)[0]

                # Dynamically add two rows and two columns to the bounding box (16 neighborhood)
                expanded_indices = (
                    slice(max(0, labeled_feature_indices[0].start - 2), min(labeled_array.shape[0], labeled_feature_indices[0].stop + 2)),
                    slice(max(0, labeled_feature_indices[1].start - 2), min(labeled_array.shape[1], labeled_feature_indices[1].stop + 2))
                )

                ahi_labels_raster_data[expanded_indices] = calculate_shift(ahi_b7_raster_data[expanded_indices],ahi_labels_raster_data[expanded_indices])
            
            log.info(f"Calculated shift for all labels in {date_str}")

            # Create a jnew output GeoTIFF file for the updated labels
            with rasterio.open(f"reprocess_data_2/input_data/himawari8/ten_minute/{date_str}/{date_lbl}_updated_ahi_labels.tif", 'w', **ahi_labels_profile) as dst:
                # Write the modified Band 1 to the output file
                dst.write(ahi_labels_raster_data, 1)
                dst.write(ahi_labels_background, 2)

            log.info(f"Updated labels for {date_str}")

    log.info("Updated labels files for 2022")
