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


def get_datetime_in_standard_format(day):
    std_form = pd.to_datetime(day)
    return std_form.strftime("%Y-%m-%dT%H:%M:%S")


def insert_rows_into_table(client, table_ref, rows):
    error = client.insert_rows_json(table_ref, rows)
    if (len(error) != 0):
        print(error)


def get_value_at_x_y_day(data, x, y, day):
    value_at_x_y_day = data.sel(x=x, y=y, time=day).dayl.values

    if np.isnan(value_at_x_y_day):
        return -1.0
    else:
        return value_at_x_y_day.item()



if __name__ == "__main__":
    project_id = 'climate-data-modeling'
    dataset_id = 'climate_data'
    table_name = 'daylength_data'
    
    insert_mode = True

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_name)

    # Actual logic to extract each value
    dl_data = xr.open_dataset('daymet_v4_daily_hi_dayl_1997.nc')
    
    tracker = 0
    batch_size = 1
    
    for day, day_index in zip(dl_data.time, list(range(0, len(dl_data.time)))):
        
        insert_mode = True
        day = day.values
        standard_date = get_datetime_in_standard_format(day)
        
        rows = []
        rows.append({
                    'date': standard_date,
                    'dayls': []
                })
    
        try:
            insert_rows_into_table(client, table_ref, rows)
            print(day_index)
            time.sleep(0.5)
        except TypeError as E:
            print(f'Type Error {E}')
