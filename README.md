# Geo-DL Data Project

This project is structured to manage and process all required data for [geo-dl-model](https://github.com/jayepraveen999/geo-dl-model). The project is organized into several directories, each with a specific purpose. All the files start with a prefix number and executed accordingly. The output of these files will be saved in data/ folder. 

The `data/` folder is not commited and also the fire_masks can't be generated as it needs the token to access the external database used for this study (OroraTech Gmbh). Henceforth all the other files cannot be executed as well because they are dependent on fire_masks. This repo serves mostly to understand the approach we followed to create the dataset for [geo-dl-model](https://github.com/jayepraveen999/geo-dl-model). 

## Project Structure

```
├── 00_config
│   └── fire_labels_auth.yml
├── 01_fire_masks
│   ├── 001_generate_fire_labels.py
│   ├── 002_filter_fire_lables.py
│   ├── 003_finalized_labels_with_fldk_cmsk_availability.py
│   └── fire_labels.py
├── 02_input_data
│   ├── 001_generate_h8_fldk_clouds.py
│   ├── 002_unzip_crop_fldk.py
│   └── aoi_h8_updated.geojson
├── 03_aux_data
│   ├── biomes
│   ├── copdem
│   └── landcover
├── 04_pre_processing
│   ├── 001_reproject_rasterize_labels.py
│   ├── 002_apply_shift_ahi_labels.py
│   ├── 003_merge_ahi_nonahi_labels.py
│   ├── 004_reproject_crop_cloud_masks.py
│   ├── 005_update_cloud_mask_on_labels.py
│   ├── 006_update_no_data_labels.py
│   ├── 007_reproject_rasterize_crop_biomes.py
│   ├── 008_reproject_resample_copdem.py
│   └── 009_reproject_resample_landcover.py
├── 05_evaluation_data
│   └── bushfires_gad_preprocessed_2022.geojson
├── 06_dataset_preparation
│   ├── create_evaluation_dataset.py
│   ├── create_training_dynamic_features_hdf5_files.py
│   └── create_training_static_features_hdf5_file.py
├── README.md
├── data
├── poetry.lock
├── pyproject.toml
└── utils.py
```

## Directory Descriptions

- **00_config**: Contains configuration files.
  - `fire_labels_auth.yml`: Authentication details and other important config data.

- **01_fire_masks**: Scripts related to fire mask generation and filtering.
  - `001_generate_fire_labels.py`: Generates fire labels.
  - `002_filter_fire_lables.py`: Filters fire labels.
  - `003_finalized_labels_with_fldk_cmsk_availability.py`: Finalizes labels with FLDK and CMSK availability.
  - `fire_labels.py`: Generic Fire Label class used to interact with the external API and an instance of this is used in `001_generate_fire_labels.py`.

- **02_input_data**: Scripts and data related to input data preparation.
  - `001_generate_h8_fldk_clouds.py`: Generates H8 FLDK cloud data.
  - `002_unzip_crop_fldk.py`: Unzips and crops FLDK data.
  - `aoi_h8_updated.geojson`: AOI in H8 projection.

- **03_aux_data**: Auxiliary data directories.
  - `biomes`: Biomes data.
  - `copdem`: Copernicus DEM data.
  - `landcover`: Land cover data.

- **04_pre_processing**: Pre-processing scripts for various data types.
  - `001_reproject_rasterize_labels.py`: Reprojects and rasterizes labels.
  - `002_apply_shift_ahi_labels.py`: Applies shift to AHI labels.
  - `003_merge_ahi_nonahi_labels.py`: Merges AHI and non-AHI labels.
  - `004_reproject_crop_cloud_masks.py`: Reprojects and crops cloud masks.
  - `005_update_cloud_mask_on_labels.py`: Updates cloud mask on labels.
  - `006_update_no_data_labels.py`: Updates labels that has no data.
  - `007_reproject_rasterize_crop_biomes.py`: Reprojects, rasterizes, and crops biomes data.
  - `008_reproject_resample_copdem.py`: Reprojects and resamples Copernicus DEM data.
  - `009_reproject_resample_landcover.py`: Reprojects and resamples land cover data.

- **05_evaluation_data**: Contains evaluation data.
  - `bushfires_gad_preprocessed_2022.geojson`: Bushfires data from Geoscience Australia of year 2022.

- **06_dataset_preparation**: Scripts for preparing datasets for training and evaluation.
  - `create_training_dynamic_features_hdf5_files.py`: Creates training/testing dataset with dynamic features in HDF5 format.
  - `create_training_static_features_hdf5_file.py`: Creates training/testing dataset with static features in HDF5 format.
  - `create_evaluation_dataset.py`: Creates the evaluation dataset.


- **data**: Directory intended for storing various data files.
- **poetry.lock**: Dependency lock file for the project.
- **pyproject.toml**: Configuration file for Python project dependencies and settings.
- **utils.py**: Utility functions used across the project.

## Installation

To set up the project, you can use Poetry, a dependency manager for Python.

1. **Install Poetry**:
   ```sh
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Install Dependencies**:
   ```sh
   poetry install
   ```
