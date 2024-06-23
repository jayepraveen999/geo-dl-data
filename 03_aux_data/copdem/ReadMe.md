Relevant filenames covering the study area are aleady copied to the copdem_tile.txt and modified the pattern of the file so that it can be used with gdal commands

* RUN THE BELOW COMMAND IN SHELL:
`gdalbuildvrt -input_file_list modified_copdem_tile.txt copdem_90m.vrt --config AWS_NO_SIGN_REQUEST YES`

* Clip the VRT to study area using the study area extent
gdal_translate -projwin 100.0 48.0 165.0 -60.0 reprocess_data/input_data/auxiliary_data/copdem/copdem_90m.vrt reprocess_data/input_data/auxiliary_data/copdem/copdem_90m_clipped.tif --config AWS_NO_SIGN_REQUEST YES