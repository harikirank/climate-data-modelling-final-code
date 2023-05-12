import geopandas as gpd
import xarray as xr
import matplotlib.pyplot as plt
import requests
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

# Define the BigQuery project and dataset IDs
project_id = 'climate-data-modeling'
dataset_id = 'python_creating_dataset'

# # Create a BigQuery client
client = bigquery.Client(project=project_id)

# # Define the BigQuery table name and schema
table_name = 'testing_table_16_apr_2023'
table_ref = client.dataset(dataset_id).table(table_name)

# Actual logic to extract each value
hawaii_daily_dl_data = xr.open_dataset('daymet_v4_daily_hi_dayl_1997.nc')

i = 0
for x, x_index in zip(hawaii_daily_dl_data.x, list(range(0, len(hawaii_daily_dl_data.x)))):
    x = x.values

    for y, y_index in zip(hawaii_daily_dl_data.y, list(range(0, len(hawaii_daily_dl_data.y)))):
        y = y.values
        
        rows = []
        rows.append(
            {
                'x':x.item(),
                'y': y.item(),
                'dayls':[]
            }
        )

        for day, day_index in zip(hawaii_daily_dl_data.time, list(range(0, len(hawaii_daily_dl_data.time)))):
    	    day = day.values

            # this is the value at a fixed x,y with the day changing.
            value_at_x_y_day = hawaii_daily_dl_data.sel(x=x,y=y,time=day)
            final_value = value_at_x_y_day.dayl.values
#             print(type(day))
            if np.isnan(final_value):
                final_value = -1.0
            
#             day_str  = day.strftime('%Y-%m-%d %H:%M:%S')
#             bq_dt = datetime.datetime.strptime(day_str, '%Y-%m-%d %H:%M:%S')
#             d_time = day.astype(datetime)
#             print(type(datetime.now()))
            

            rows[0]['dayls'].append(
                {'date': '2022-01-01 00:00:00', 'dayl': final_value}
            )
            
#             print(f'value at x: {x}, y: {y}, date: {day} is {value_at_x_y_day.dayl.values}')
#             i += 1
#             if i == 100:
#                 break

        print(rows)
        break
    break

table_ref = client.dataset(dataset_id).table(table_name)
error = client.insert_rows_json(table_ref, rows)
print(error)
