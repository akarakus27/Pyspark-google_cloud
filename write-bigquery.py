from google.oauth2 import service_account
from google.cloud import bigquery

# Create Authentication Credentials
project_id = "test-applications-xxxxx"
table_id = f"{project_id}.test_dataset.user_details_python_csv"
gcp_credentials = service_account.Credentials.from_service_account_file('test-applications-xxxxx-74dxxxxx.json')

# Create BigQuery Client
bq_client = bigquery.Client(credentials=gcp_credentials)

# Create Table Schema
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("address", "STRING"),
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

# CSV File Location (Cloud Storage Bucket)
uri = "https://storage.cloud.google.com/test_python_functions/user_details.csv"

# Create the Job
csv_load_job = bq_client.load_table_from_uri(
    uri, table_id, job_config=job_config
)

csv_load_job.result()