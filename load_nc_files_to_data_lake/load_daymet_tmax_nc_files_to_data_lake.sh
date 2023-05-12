#!/bin/bash

echo "downloading the files"

root_url="https://thredds.daac.ornl.gov/"
path_to_file="thredds/fileServer/ornldaac/2129/"

BUCKET_NAME="data-lake-bucket-climate-modelling"
directory_name="maximumtemperature"

# Extract and store tmax data from 1980 to 2022 into gcloud gcp bucket
for i in {1980..2022}
do
    for locations in "hi" "pr" "na"
    do
        nc_file_name="daymet_v4_daily_${locations}_tmax_$i.nc"
        url_to_download_file=$root_url$path_to_file$nc_file_name
        
        wget -O "$nc_file_name" $url_to_download_file
        
        gsutil cp "$nc_file_name" "gs://$BUCKET_NAME/$directory_name"
        
        rm $nc_file_name

	echo "Successfully downloaded and uploaded $nc_file_name"
    done
done

echo "Done downloading and uploading the netcdf  files"
