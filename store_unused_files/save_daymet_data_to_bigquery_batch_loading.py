#!/usr/bin/python3
import netCDF4
import pandas as pd
from google.cloud import bigquery

# Define the BigQuery project and dataset IDs
project_id = 'climate-data-modeling'
dataset_id = 'python_creating_dataset'

# Define the BigQuery table name and schema
table_name = 'testing_table'
table_schema = [
    bigquery.SchemaField('time', 'TIMESTAMP'),
    bigquery.SchemaField('lat', 'FLOAT'),
    bigquery.SchemaField('lon', 'FLOAT'),
    bigquery.SchemaField('dayl', 'FLOAT')
]

# Open the netCDF file
nc_file = netCDF4.Dataset('daymet_v4_daily_hi_dayl_2022.nc')

# Extract data from the netCDF file
time_var = nc_file.variables['time']
lat_var = nc_file.variables['lat']
lon_var = nc_file.variables['lon']
dayl_var = nc_file.variables['dayl']
time_data = pd.to_datetime(netCDF4.num2date(time_var[:], time_var.units, only_use_cftime_datetimes=False, only_use_python_datetimes=True), format="%Y-%m-%d")
lat_data = lat_var[:]
lon_data = lon_var[:]
dayl_data = dayl_var[:]

# Create a pandas DataFrame with the extracted data
df = pd.DataFrame({
    'time': time_data,
    'lat': lat_data.ravel(),
    'lon': lon_data.ravel(),
    'dayl': dayl_data.ravel()
})

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Create a new table in BigQuery
table_ref = client.dataset(dataset_id).table(table_name)
table = bigquery.Table(table_ref, schema=table_schema)
table = client.create_table(table)

# Load the data into the BigQuery table
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

# Wait for the job to complete
job.result()

# Print the number of rows that were inserted
print(f"Loaded {job.output_rows} rows into BigQuery table {table_name}")
