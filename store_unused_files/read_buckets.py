from google.cloud import storage

project_id = 'climate-data-modeling'

dataset_id = 'climate_data'
table_name = 'daylength_data_2'

bucket_name = 'data-lake-bucket-climate-modelling'
folder_path = 'daylength/'

# Create a client
client = storage.Client(project=project_id)

# Get the bucket
bucket = client.get_bucket(bucket_name)

# List all the files in the folder
blobs = bucket.list_blobs(prefix=folder_path)

# Process each file
for blob in blobs:
    if blob.name.endswith('.nc'):
        print(blob.name)
