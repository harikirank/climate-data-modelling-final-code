import sys

import geopandas as gpd
import xarray as xr
import matplotlib.pyplot as plt
import requests
import pandas as pd
import pyproj
import datetime as dt
import numpy as np
from rasterio.features import Affine
from pydap.client import open_url
from pydap.cas.urs import setup_session
import rioxarray
import netCDF4 as nc
import shapely
import datetime as dt
import rasterio
import time
from shapely.geometry import mapping
from google.cloud import bigquery
from datetime import datetime
from google.cloud import storage
import os


def get_datetime_in_standard_format(day):
    std_form = pd.to_datetime(day)
    return std_form.strftime("%Y-%m-%dT%H:%M:%S")


def insert_rows_into_table(client, table_ref, rows):
    error = client.insert_rows_json(table_ref, rows)
    if len(error) != 0:
        print(error)


def get_value_at_x_y_day(data, x, y, day):
    value_at_x_y_day = data.sel(x=x, y=y, time=day).dayl.values

    if np.isnan(value_at_x_y_day):
        return -1.0
    else:
        return value_at_x_y_day.item()


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='', flush=True)

if __name__ == "__main__":
    project_id = 'climate-data-modeling'
    dataset_id = 'climate_data'

    bucket_name = 'data-lake-bucket-climate-modelling'
    # This folder contains both the dayl and prcp files
    folder_paths = ['daylength/', 'maximumtemperature/', 'minimumtemperature/', 'shortwaveradiation/',
                    'snowwaterequivalent/']
    # Create a client
    storage_client = storage.Client(project=project_id)
    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Process each file in each folder
    for folder_path in folder_paths:
        # List all the files in the folder
        blobs = bucket.list_blobs(prefix=folder_path)
        # Create the directory if it doesn't exist
        os.makedirs('/tmp/daylength/', exist_ok=True)
        for blob in blobs:
            if blob.name.endswith('.nc'):
                file_name = blob.name.split(".")[0].split("/")[1]
                measured_region = file_name.split("_")[3]
                measured_value = file_name.split("_")[4]
                measured_year = file_name.split("_")[5]

                # For now only creating the dayl table. We should ideally be creating all the tables for all the
                # values available.
                if measured_value == 'tmax':
                    table_name = f"{measured_region}_{measured_value}_{measured_year}"

                    client = bigquery.Client(project=project_id)
                    table_ref = client.dataset(dataset_id).table(table_name)

                    temp_file_path = '/tmp/' + blob.name
                    blob.download_to_filename(temp_file_path)
                    dl_data = xr.open_dataset(temp_file_path)

                    for day, day_index in zip(dl_data.time, list(range(0, len(dl_data.time)))):

                        total_iterations = len(dl_data.time)

                        day = day.values
                        standard_date = get_datetime_in_standard_format(day)

                        rows = [{
                            'date': standard_date,
                            'dayls': []
                        }]

                        try:
                            insert_rows_into_table(client, table_ref, rows)

                            print(f"Progress: Inserting Date: {standard_date} into Table: {table_name}", end="\r")
                            # Calculate the progress percentage
                            progress = (day_index + 1) / total_iterations

                            # Print the progress bar
                            print_progress_bar(day_index + 1, total_iterations, prefix='Progress:', suffix='Complete ')
                        except TypeError as E:
                            print(f'Type Error {E}')

