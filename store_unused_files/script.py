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
    dataset_id = 'python_creating_dataset'
    table_name = 'testing_table_3_16_apr_2023'

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_name)

    # Actual logic to extract each value
    dl_data = xr.open_dataset('daymet_v4_daily_hi_dayl_1997.nc')
    
    tracker = 0
    batch_size = 50
    
    for day, day_index in zip(dl_data.time, list(range(0, len(dl_data.time)))):
        day = day.values
        standard_date = get_datetime_in_standard_format(day)
        
        rows = []
        i = 0
        
        for x, x_index in zip(dl_data.x, list(range(0, len(dl_data.x)))):
            x = x.values
            
            for y, y_index in zip(dl_data.y, list(range(0, len(dl_data.y)))):
                y = y.values
                
                rows.append({
                    'date': standard_date,
                    'dayls': []
                })
                
                value_at_x_y_day = get_value_at_x_y_day(dl_data, x, y, day)
                if value_at_x_y_day != -1.0:
                    rows[day_index]['dayls'].append(
                        {'x':x.item(), 'y': y.item(), 'dayl': value_at_x_y_day} 
                        )
                        
                    tracker += 1
                    print(tracker)
                    if (tracker == batch_size):
                        print("enter into if block")
                        insert_rows_into_table(client, table_ref, rows)
                        print("Inserted rows successfully.")
                        
                        tracker = 0
                        
                        rows = []
                        rows.append({
                            'date': get_datetime_in_standard_format(day),
                            'dayls': []
                        })
                    
                        
                    # print(f"Inserted data at {x}, {y}, {day} {value_at_x_y_day}")    
                # else:
                    # print(f"Nan value at {x}, {y}, {day}")
    
        try:
            insert_rows_into_table(client, table_ref, rows)
        except TypeError as E:
            print(f'Type Error {E}')
