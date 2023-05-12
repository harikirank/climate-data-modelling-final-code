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
from google.cloud import storage
from datetime import datetime
import os


def get_datetime_in_standard_format(day):
    """
    Converts the given date to a format of %Y-%m-%dT%H:%M:%S
    :param day: The date that is in the netcdf file
    :return: The given date converted to a format of %Y-%m-%dT%H:%M:%S
    """
    std_form = pd.to_datetime(day)
    return std_form.strftime("%Y-%m-%dT%H:%M:%S")


def insert_rows_into_table(client, table_ref, rows):
    """
    Tries to insert the given rows into the bigquery data warehouse. Prints the error if an error occurs. Doesn't print
    anything if the data insertion into the warehouse is successful
    :param client: the bigquery client used to make changes to the table
    :param table_ref: The table reference to insert the values into
    :param rows: The rows that need to be inserted into the bigquery
    :return: None. Prints the error if the insertion fails
    """
    error = client.insert_rows_json(table_ref, rows)
    if len(error) != 0:
        print(error)


def get_value_at_x_y_day(data, x, y, date):
    """
    Given the Xarray Dataset, coordinates x,y and the day, returns the value at the specified x,y and date.
    :param data: The Xarray dataset
    :param x: The X coordinate
    :param y: The Y coordinate
    :param date: The date of the value
    :return: returns the value at the specified x, y and date. Here the value can be dayl, tmin, tmax, swe etc.
    """
    value_at_x_y_day = data.sel(x=x, y=y, time=date).dayl.values

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
    project_id: str = 'climate-data-modeling'
    dataset_id: str = 'climate_data'

    # bucket name and the folder path where the NC files are stored
    bucket_name: str = 'data-lake-bucket-climate-modelling'
    # This folder contains both the dayl and prcp files
    folder_paths: list = ['daylength/', 'maximumtemperature/', 'minimumtemperature/', 'shortwaveradiation/',
                          'snowwaterequivalent/']

    # Create a client
    storage_client = storage.Client(project=project_id)
    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    counter: int = 0
    batch_size: int = 100
    current_batch = 1

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

                if measured_value == 'dayl':
                    table_name = f"{measured_region}_{measured_value}_{measured_year}"

                    # The bigquery client and the table reference
                    client = bigquery.Client(project=project_id)
                    table_ref = client.dataset(dataset_id).table(table_name)

                    temp_file_path = '/tmp/' + blob.name
                    blob.download_to_filename(temp_file_path)
                    dl_data = xr.open_dataset(temp_file_path)

                    for day, day_index in zip(dl_data.time, list(range(0, len(dl_data.time)))):

                        insert_mode = False
                        day = day.values
                        standard_date = get_datetime_in_standard_format(day)

                        rows = [{
                            'date': standard_date,
                            'dayls': []
                        }]

                        i = 0

                        for x, x_index in zip(dl_data.x, list(range(0, len(dl_data.x)))):
                            x = x.values

                            for y, y_index in zip(dl_data.y, list(range(0, len(dl_data.y)))):

                                y = y.values

                                value_at_x_y_day = get_value_at_x_y_day(dl_data, x, y, day)
                                if value_at_x_y_day != -1.0:
                                    rows[day_index]['dayls'].append(
                                        {'x': x.item(), 'y': y.item(), 'dayl': value_at_x_y_day}
                                    )

                                    counter += 1
                                    if counter == batch_size:
                                        print_progress_bar(current_batch,
                                                           len(dl_data.x) * len(dl_data.y) / batch_size,
                                                           prefix=f'Processing Batch {current_batch}, inserting '
                                                                  f'values into table {table_name} and '
                                                                  f'date {standard_date}'
                                                                  f' row, total '
                                                                  f'batches: '
                                                                  f'{int(len(dl_data.x) * len(dl_data.y) / batch_size) + 1}, '
                                                                  f'total '
                                                                  f'values to be inserted into {standard_date}: '
                                                                  f'{len(dl_data.x) * len(dl_data.y)}',
                                                           suffix='Complete ')
                                        current_batch += 1

                                        # Fetch the existing values of the nested field
                                        nested_field = 'dayls'
                                        condition = f"date = '{standard_date}'"

                                        # Build the update SQL statement
                                        repeated_field = 'dayls'

                                        x_y_dayl_values = rows[0]['dayls']

                                        update_statement = ', '.join([
                                            f"STRUCT({row['x']} AS x, {row['y']} AS y, {round(row['dayl'], 2)} AS dayl)"
                                            for row in
                                            x_y_dayl_values
                                        ])
                                        sql = f"""
                                            UPDATE `{project_id}.{dataset_id}.{table_name}`
                                            SET {repeated_field} = ARRAY_CONCAT({repeated_field}, [{update_statement}])
                                            WHERE {condition}
                                        """

                                        # Execute the update statement
                                        query_job = client.query(sql)
                                        query_job.result()  # Wait for the query to complete

                                        # Check the query job for errors
                                        if query_job.error_result is not None:
                                            raise Exception(f"Query execution failed: {query_job.error_result}")

                                        counter = 0
                                        rows = [{
                                            'date': standard_date,
                                            'dayls': []
                                        }]

