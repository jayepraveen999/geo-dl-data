from datetime import datetime, timedelta
import glob
import rasterio
import numpy as np
import h5py
import json
import logging as log
import warnings

log_file = f"train_test_split_data_files/test_split/dynamic_files/create_testing_dynamic_features_hdf5_files.txt"  # Path to the log file

log.basicConfig(
    filename=log_file,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)



# get child timestamps
def get_child_timestamps(timestamp_str:str) -> list[str]:
    """
    Returns the last three 10-minute intervals for the given timestamp
    """
    # Convert the timestamp string to a datetime object
    timestamp = datetime.strptime(timestamp_str, '%Y/%m/%d/%H%M/')

    # Calculate the last three 10-minute intervals
    intervals = []
    for i in range(1, 4):
        new_time = timestamp - timedelta(minutes=i * 10)
        intervals.append(new_time.strftime('%Y/%m/%d/%H%M/'))
    intervals.append(timestamp_str)

    return sorted(intervals)


def write_to_hdf5(timestamp_batch, sample_size):

    no_of_samples= sample_size
    no_of_bands = 6
    timeseries_length = 4
    sample_width,sample_height = 256,256   
    timestamps_str = np.empty((no_of_samples, timeseries_length, 1), dtype='S20')
    ahi_data = np.empty((no_of_samples, timeseries_length, no_of_bands, sample_width,sample_height), dtype=np.float32)
    cloud_mask_binary = np.empty((no_of_samples, sample_width,sample_height), dtype=np.int8)
    ahi_stat_p = np.empty((no_of_samples, no_of_bands, 2), dtype=np.float32)
    ahi_stat_p_c = np.empty((no_of_samples, timeseries_length, no_of_bands, 2), dtype=np.float32)
    fire_fraction = np.empty((no_of_samples, 1), dtype=np.float32)
    cloud_fraction = np.empty((no_of_samples, 1), dtype=np.float32)
    raster_window_id = np.empty((no_of_samples, 1), dtype=np.int8)
    labels_data = np.empty((no_of_samples, sample_width, sample_height), dtype=np.int8)

    log.info(f"Allocated memory for all the arrays in this timestamp batch: timestamps_str: {timestamps_str.shape}, ahi_data: {ahi_data.shape}, cloud_mask_binary: {cloud_mask_binary.shape}, ahi_stat_p: {ahi_stat_p.shape}, ahi_stat_p_c: {ahi_stat_p_c.shape}, fire_fraction: {fire_fraction.shape}, cloud_fraction: {cloud_fraction.shape}, raster_window_id: {raster_window_id.shape}")

    sample_count = 0
    for timestamp in timestamp_batch:

        # get ahi_data and timestamps_data (as minute of the year)
        child_timestamps = get_child_timestamps(timestamp)
        # reversing the order so the parent timestamp comes first
        child_timestamps = child_timestamps[::-1]

        ahi_filenames=[]
        timestamps_data = []
        for child_timestamp in child_timestamps:
            timestamp_str_tmp = f"{timestamp}{child_timestamp.split('/')[-2]}"
            stacked_masked = sorted(glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp_str_tmp}/*_stacked_masked.tif"))
            if stacked_masked:
                ahi_filenames.append(stacked_masked[0])

                # write the timestamp so that it is used as timestamp_str (product name)
                timestamps_data.append(timestamp_str_tmp)
       
        for timeseries, ahi_filename in enumerate(ahi_filenames):
            ahi_stacked_data = rasterio.open(ahi_filename)
            count=sample_count
            for window in RASTER_WINDOWS:

                # write timestamp_str
                timestamps_str[count,timeseries,:] = np.array(timestamps_data[timeseries], dtype='S20')

                # write ahi_data
                tile = ahi_stacked_data.read(window=rasterio.windows.Window(window[1], window[0], 256, 256))
                ahi_data[count,timeseries,:,:,:] = tile

                # write ahi_stat_p, ahi_stat_p_c
                ahi_stat = np.transpose([np.mean(tile, axis=(1,2)), np.std(tile, axis=(1,2))])
                if timeseries==0:
                    ahi_stat_p[count,:,:] = ahi_stat
                ahi_stat_p_c[count,timeseries,:,:] = ahi_stat
                count+=1

                
                
        cmsk_filename = glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}{timestamp.split('/')[-2]}/cd_mask_*.tif")
        if cmsk_filename:
            cmsk_file = rasterio.open(cmsk_filename[0])
            count = sample_count
            for window_id,window in enumerate(RASTER_WINDOWS):
                tile = cmsk_file.read(2, window=rasterio.windows.Window(window[1], window[0], 256, 256))

                # change the dtype to int8
                tile = tile.astype(np.int8)

                # write cloud_mask_binary
                cloud_mask_binary[count,:,:] = tile

                # write cloud_fraction
                cloud_fraction[count,:] = np.count_nonzero(tile==1)/(256*256)

                # write raster_window_id
                raster_window_id[count,:] = window_id
                count+=1

        # calculate fire_fraction
        fire_file = glob.glob(f"reprocess_data_2/input_data/himawari8/ten_minute/{timestamp}*_cmsk_applied_labels_with_nan.tif")
        if fire_file:
            fire_labels = rasterio.open(fire_file[0])
            count = sample_count
            for window in RASTER_WINDOWS:
                tile = fire_labels.read(1, window=rasterio.windows.Window(window[1], window[0], 256, 256))

                # write to labels
                labels_data[count,:,:] = tile

                # write to fire_fraction
                fire_fraction[count,:] = np.count_nonzero(tile==1)/(256*256)
                count+=1
                
        sample_count+=len(RASTER_WINDOWS)

    # write log warning if sample_coubt is not equal to no_of_samples
    if sample_count != no_of_samples:
        log.warning(f"Sample count: {sample_count} is not equal to no_of_samples: {no_of_samples}")
        # raise exception and stop the execution
        raise Exception(f"Sample count: {sample_count} is not equal to no_of_samples: {no_of_samples}")
    
    # print all the shapes
    log.info(f"Shape of all the arrays: timestamps_str: {timestamps_str.shape}, ahi_data: {ahi_data.shape}, cloud_mask_binary: {cloud_mask_binary.shape}, ahi_stat_p: {ahi_stat_p.shape}, ahi_stat_p_c: {ahi_stat_p_c.shape}, fire_fraction: {fire_fraction.shape}, cloud_fraction: {cloud_fraction.shape}, raster_window_id: {raster_window_id.shape}, labels_data: {labels_data.shape}")
    return timestamps_str, ahi_data, cloud_mask_binary, ahi_stat_p, ahi_stat_p_c, fire_fraction, cloud_fraction, raster_window_id, labels_data
    


# get the timestamps
with open("reprocess_data_2/fire_labels/ten_minute/unique_dates_ten_minute_finalized.json") as json_file:
    timestamps = json.load(json_file)

testing_timestamps = []
for timestamp in timestamps:
    if timestamp.startswith("2022"):
        testing_timestamps.append(timestamp)

RASTER_WINDOWS = [(1024, 256), (1280, 256), (1536, 256), (1792, 256), (2048, 256), (2304, 256), (2560, 256), (512, 512), (768, 512), (1024, 512), (1536, 512), (1792, 512), (2048, 512), (2304, 512), (2560, 512), (3072, 512), (3328, 512), (3584, 512), (256, 768), (512, 768), (768, 768), (1024, 768), (1280, 768), (1536, 768), (1792, 768), (2048, 768), (2304, 768), (2560, 768), (3072, 768), (3328, 768), (3584, 768), (3840, 768), (256, 1024), (512, 1024), (768, 1024), (1024, 1024), (1280, 1024), (1536, 1024), (1792, 1024), (2048, 1024), (2304, 1024), (2560, 1024), (2816, 1024), (3072, 1024), (3328, 1024), (3584, 1024), (3840, 1024), (256, 1280), (512, 1280), (768, 1280), (2048, 1280), (2304, 1280), (2560, 1280), (2816, 1280), (3072, 1280), (3328, 1280), (3584, 1280), (3840, 1280), (256, 1536), (512, 1536), (1792, 1536), (2048, 1536), (2304, 1536), (2560, 1536), (2816, 1536), (3072, 1536), (3328, 1536), (3584, 1536), (3840, 1536), (256, 1792), (512, 1792), (1536, 1792), (2048, 1792), (2304, 1792), (2560, 1792), (2816, 1792), (3072, 1792), (3328, 1792), (3584, 1792), (3840, 1792), (4096, 1792), (1280, 2048), (2048, 2048), (2304, 2048), (2560, 2048), (2816, 2048), (3072, 2048), (3328, 2048), (3584, 2048), (3840, 2048), (4096, 2048), (2048, 2304), (2304, 2304), (2560, 2304), (3072, 2304), (3328, 2304), (3584, 2304), (3840, 2304), (4352, 2304), (2304, 2560), (2560, 2560), (2816, 2560), (3328, 2560), (3584, 2560), (1792, 2816), (2560, 2816), (2816, 2816)]
log.info(f"Succesfully loaded the timestamps and raster windows")

log.info(f"Chuncking the timestamps with chunk size of 120 and writing to hdf5 files")
# read 120 timestamps at a time
file_naming_start = 0
file_naming_end = 0
for i in range(0, len(testing_timestamps), 120):
    timestamp_batch = testing_timestamps[i:i+120]

    # just have one timestamp for testing
    # timestamp_batch = timestamp_batch[:1]

    sample_size = len(timestamp_batch) * 107


    file_naming_end = file_naming_start + sample_size
    log.info(f"file_naming_start: {file_naming_start}, file_naming_end: {file_naming_end}")
    log.info(f"Timestamp batch start and end: {timestamp_batch[0]} and {timestamp_batch[-1]}")

    # # check if the .h5 file already exists
    # if glob.glob(f'train_test_split_data_files/train_split/dynamic_files/training_dynamic_data_{file_naming_start}_{file_naming_end}.h5'):
    #     log.info(f"File training_dynamic_data_{file_naming_start}_{file_naming_end}.h5 already exists. Skipping this batch")
    #     continue
    
    timestamps_str, ahi_data, cloud_mask_binary, ahi_stat_p, ahi_stat_p_c, fire_fraction, cloud_fraction, raster_window_id, labels_data = write_to_hdf5(timestamp_batch, sample_size)
    log.info(f"Finished creating all data arrays for this timestamp batch")
    
    # write to hdf5
    filename = f'train_test_split_data_files/test_split/dynamic_files/testing_dynamic_data_{file_naming_start}_{file_naming_end}.h5'
    # mnt_filename = f'/mnt/research/datasets/thesis-jayendra/testing_hdf_files/testing_dynamic_data_{file_naming_start}_{file_naming_end}.h5'

    with h5py.File(filename, 'w') as f:

        # create input features group
        input_features = f.create_group('input_features')

        input_features.create_dataset('timestamps_str', data=timestamps_str, chunks = (1,4,1), compression="lzf")
        input_features.create_dataset('ahi_data', data=ahi_data, chunks = (1,4,6,256,256), compression="lzf")
        input_features.create_dataset('cloud_mask_binary', data=cloud_mask_binary, chunks = (1,256,256), compression="lzf")
        input_features.create_dataset('ahi_stat_p', data=ahi_stat_p, chunks = (1,6,2), compression="lzf")
        input_features.create_dataset('ahi_stat_p_c', data=ahi_stat_p_c, chunks = (1,4,6,2), compression="lzf")
        input_features.create_dataset('fire_fraction', data=fire_fraction, chunks = (1,1), compression="lzf")
        input_features.create_dataset('cloud_fraction', data=cloud_fraction, chunks = (1,1), compression="lzf")
        input_features.create_dataset('raster_window_id', data=raster_window_id, chunks = (1,1), compression="lzf")
        # also write description for each dataset 
        input_features['timestamps_str'].attrs['description'] = "Timestamps for each sample. Includes Parents timestamp at index 0 and children timestamps at index 1,2,3"
        input_features['ahi_data'].attrs['description'] = "AHI data for each sample. Includes 6 bands for each timestamp"
        input_features['cloud_mask_binary'].attrs['description'] = "Cloud mask binary for each sample. Cloud mask indicates the mask for parent timestamp"
        input_features['ahi_stat_p'].attrs['description'] = "AHI statistics for parent timestamp. Includes mean and standard deviation for each band"
        input_features['ahi_stat_p_c'].attrs['description'] = "AHI statistics for parent and child timestamp. Includes mean and standard deviation for each band"
        input_features['fire_fraction'].attrs['description'] = "Fire fraction for each sample. Fire fraction indicates the fraction of fire pixels in the parent timestamp"
        input_features['cloud_fraction'].attrs['description'] = "Cloud fraction for each sample. Cloud fraction indicates the fraction of cloud pixels in the parent timestamp"
        input_features['raster_window_id'].attrs['description'] = "Raster window id for each sample. Raster window id indicates the window id in the raster image and used as foregin key to get the static features for each sample"

        # create labels group
        labels_group = f.create_group('labels')
        labels_group.create_dataset('labels_data', data=labels_data, chunks = (1,256,256), compression="lzf")
        labels_group['labels_data'].attrs['description'] = "Labels for each sample. Labels indicate the presence of fire pixels in the parent timestamp"

    log.info(f"Finished writing to hdf5 file for this timestamp batch: testing_dynamic_data_{file_naming_start}_{file_naming_end}.h5")
    file_naming_start = file_naming_end 

   
    # break

# filename explanation:
# training_dynamic_data_0_12840.h5: This filenaming mean that the samples from 0 to 12840 are stored in this file (generally there is no sample 0 but here it indicates sample 1 as we apply numpy index naming convention). So to get samle 4352, we need to open the file training_dynamic_data_0_12840.h5 and get the sample at index 4351.

