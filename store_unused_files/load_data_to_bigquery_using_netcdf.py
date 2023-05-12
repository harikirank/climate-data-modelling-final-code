import netCDF4
from google.cloud import bigquery

# Set up a BigQuery client
client = bigquery.Client()

# Open the netCDF file
nc_file = netCDF4.Dataset('daymet_v4_daily_hi_dayl_2022.nc')

# Get the variable names and dimensions
var_names = nc_file.variables.keys()
dim_names = list(nc_file.dimensions.keys())

# Define the BigQuery schema for the data
schema = []
for var_name in var_names:
    dtype = str(nc_file.variables[var_name].dtype)
    if dtype == 'float32':
        field_type = 'FLOAT'
    elif dtype == 'int32':
        field_type = 'INTEGER'
    else:
        field_type = 'STRING'
    schema.append(bigquery.SchemaField(var_name, field_type))

# Create a BigQuery table with the specified schema
table_id = 'climate-data-modeling.python_creating_dataset.testing_table'
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)

# Define the batch size for loading data into BigQuery
batch_size = 1

# Load the data into BigQuery in batches
rows = []
for i in range(nc_file.dimensions[dim_names[0]].size):
    row = {}
    for var_name in var_names:
        row[var_name] = nc_file.variables[var_name][i].tolist()
    rows.append(row)
    if len(rows) == batch_size:
        errors = client.insert_rows_json(table, rows)
        if errors:
            print(errors)
        rows = []
if len(rows) > 0:
    errors = client.insert_rows_json(table, rows)
    if errors:
        print(errors)

# Close the netCDF file
nc_file.close()
