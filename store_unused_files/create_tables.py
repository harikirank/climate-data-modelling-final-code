from google.cloud import bigquery

# Set your Google Cloud project ID
project_id = 'climate-data-modeling'
dataset_id = 'climate_data'

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Define the schema for the table
schema = [
    bigquery.SchemaField("date", "DATETIME"),
    bigquery.SchemaField("dayls", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("x", "FLOAT64"),
        bigquery.SchemaField("y", "FLOAT64"),
        bigquery.SchemaField("dayl", "FLOAT64"),
    ]),
]

# Define the table reference
table_ref = client.dataset(dataset_id).table("your_table_name")

# Define the table definition
table = bigquery.Table(table_ref, schema=schema)

# Create the table in BigQuery
client.create_table(table)
