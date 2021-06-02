import json 
from google.cloud import bigquery
from datetime import datetime, timedelta
import os
from time import time

databaseSchema = "your_proyect_id.dataset_id"
table_id = databaseSchema + 'your_table_id'

#get or generate your data and p


def save_to_database(tableId, jsonInfoToLoad):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(
        jsonInfoToLoad, table_id
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

save_to_database(table_id, data) # you have to put in "data" the data you want to upload to bigquery