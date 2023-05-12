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
    
    insert_mode = True

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_name)

    # Actual logic to extract each value
    dl_data = xr.open_dataset('daymet_v4_daily_hi_dayl_1997.nc')
    
    tracker = 0
    batch_size = 50
    
    for day, day_index in zip(dl_data.time, list(range(0, len(dl_data.time)))):
        
        insert_mode = True
        day = day.values
        standard_date = get_datetime_in_standard_format(day)
        
        rows = []
        rows.append({
                    'date': standard_date,
                    'dayls': []
                })
        
        i = 0
        
        for x, x_index in zip(dl_data.x, list(range(0, len(dl_data.x)))):
            x = x.values
            
            for y, y_index in zip(dl_data.y, list(range(0, len(dl_data.y)))):
                y = y.values
                
                
                value_at_x_y_day = get_value_at_x_y_day(dl_data, x, y, day)
                if value_at_x_y_day != -1.0:
                    rows[day_index]['dayls'].append(
                        {'x':x.item(), 'y': y.item(), 'dayl': value_at_x_y_day} 
                        )
                        
                    tracker += 1
                    print(tracker)
                    if (tracker == batch_size):
                        # Fetch the existing values of the nested field
                        nested_field = 'dayls'
                        condition = f"date = '{standard_date}'"
                        select_statement = f"""
                            SELECT {nested_field}
                            FROM `{project_id}.{dataset_id}.{table_name}` WHERE {condition}
                        """

                        # query_job = client.query(select_statement)
                        # results = query_job.result()
                        
                        # row = []
                        # existing_values = []
                        
                        # try:
                        #     row = next(results)
                        #     print(rows)
                            
                        #     existing_values = row[nested_field]
                        #     rows = existing_values + rows
                        #     # Process the row or perform any desired operations
                        # except StopIteration:
                        #     # Handle the case when there are no more rows
                        #     print("No more rows to fetch.")
                        
                        
                        
                        print("enter into if block")
                        if insert_mode:
                            insert_rows_into_table(client, table_ref, rows)
                            insert_mode = False
                            print("Inserted rows successfully.")
                        else:
                            # Build the update SQL statement
                            repeated_field = 'dayls'
                            
                            x_y_dayl_values = rows[0]['dayls']
                            print(x_y_dayl_values)
                            update_statement = ', '.join([
    f"STRUCT({row['x']} AS x, {row['y']} AS y, {row['dayl']} AS dayl)" for row in x_y_dayl_values
])
                            sql = f"""
                                UPDATE `{project_id}.{dataset_id}.{table_name}`
                                SET {repeated_field} = ARRAY_CONCAT({repeated_field}, [{update_statement}])
                                WHERE {condition}
                            """
                            print(sql)
                            # Define the query job configuration
                            
                            # job_config = bigquery.QueryJobConfig()
                            # job_config.query_parameters = [
                            #     bigquery.ArrayQueryParameter("new_values", "FLOAT64", rows)
                            # ]
                            
                            # Execute the update statement
                            query_job = client.query(sql)
                            query_job.result()  # Wait for the query to complete
                            
                            # Check the query job for errors
                            if query_job.error_result is not None:
                                raise Exception(f"Query execution failed: {query_job.error_result}")
                            
                        tracker = 0
                        rows = []
                        rows.append({
                            'date': standard_date,
                            'dayls': []
                        })
                    
                        
                    # print(f"Inserted data at {x}, {y}, {day} {value_at_x_y_day}")    
                # else:
                    # print(f"Nan value at {x}, {y}, {day}")
    
        try:
            insert_rows_into_table(client, table_ref, rows)
        except TypeError as E:
            print(f'Type Error {E}')
