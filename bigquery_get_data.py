import json 
from google.cloud import bigquery
from datetime import datetime, timedelta
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials.json"

#bigquery uses sql querys, so you must set up the query like this
#for this query lets supose we have a field called date and other called revenue

def get_data_from_bigquery(fromDate, toDate):
        client = bigquery.Client()        
        query = """
            SELECT date, revenue
            FROM `proyect_id.dataset_id.table_id`
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(None, "STRING", fromDate),
                bigquery.ScalarQueryParameter(None, "STRING", toDate),
            ]
        )
        
        query_job = client.query(query, job_config)
        results = query_job.result()  # Waits for job to complete.
        revenue_by_date = []
        for row in results:
            revenue_by_date.append({
                "date": row.date,
                "revenue": row.revenue
            })    

        return revenue_by_date