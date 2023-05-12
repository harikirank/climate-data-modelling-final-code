from google.cloud import storage
from google.cloud import bigquery

project_id = 'climate-data-modeling'

dataset_id = 'climate_data'

bucket_name = 'data-lake-bucket-climate-modelling'

# This folder contains both the dayl and prcp files
folder_paths = ['daylength/','maximumtemperature/', 'minimumtemperature/', 'shortwaveradiation/', 'snowwaterequivalent/']

# Create a client
client = storage.Client(project=project_id)
# Get the bucket
bucket = client.get_bucket(bucket_name)

# Create a bigquery Client
bq_client = bigquery.Client(project=project_id)

# Process each file in each folder
for folder_path in folder_paths:
    # List all the files in the folder
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        if blob.name.endswith('.nc'):
            file_name = blob.name.split(".")[0].split("/")[1]
            measured_region = file_name.split("_")[3]
            measured_value = file_name.split("_")[4]
            measured_year = file_name.split("_")[5]
            
            # For now only creating the dayl table. We should ideally be creating all the tables for all the values available.
            if measured_value == 'prcp':
                table_name = f"{measured_region}_{measured_value}_{measured_year}"
                
                # Now create the table with the table name
                # Define the schema for the table
                schema = [
                    bigquery.SchemaField("date", "DATETIME"),
                    bigquery.SchemaField("dayls", "RECORD", mode="REPEATED", fields=[
                        bigquery.SchemaField("x", "FLOAT64"),
                        bigquery.SchemaField("y", "FLOAT64"),
                        bigquery.SchemaField("prcp", "FLOAT64"),
                    ]),
                ]
                
                # Define the table reference
                table_ref = bq_client.dataset(dataset_id).table(table_name)
                
                # Define the table definition
                table = bigquery.Table(table_ref, schema=schema)
                
                # Create the table in BigQuery
                bq_client.create_table(table)
            

