from google.cloud.orchestration.airflow import DAG, ObjectStoragePath, PythonOperator
from google.cloud import LocalFilesystemToGCSOperator
from google.cloud import BigQueryInsertJobOperator
from google.cloud import storage
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

base_uri = ObjectStoragePath(os.get("BUCKET_URI"))
bucket_name = os.get("BUCKET_NAME")
project_id = os.get("PROJECT_ID")
dataset_id = os.get("DATASET_ID")
table_id = os.get("TABLE_ID")

default_args = {
    "start_date": datetime(2025, 7, 5),
    "retries": 1,
}

with DAG(
    dag_id="voter_data_etl",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL pipeline voter data to BigQuery",
) as dag:

    def clean_csv_for_bigquery(src_path, cleaned_path, **kwargs):
        """
        Cleans the CSV file for BigQuery insertion.
        """
        df = pd.read_csv(src_path)
        # Strip whitespace from headers and values
        df.columns = [col.strip() for col in df.columns]
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.where(pd.notnull(df), None)
        df.to_csv(cleaned_path, index=False)

    clean_task = PythonOperator(
        task_id="clean_csv",
        python_callable=clean_csv_for_bigquery,
        op_kwargs={
            "src_path": "voter_data.csv",
            "cleaned_path": "/tmp/cleaned_data.csv"
        }
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/cleaned_data.csv",
        dst="etl/cleaned_data.csv",
        bucket=bucket_name
    )

    bq_load = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [
                    f"{base_uri}/path/to/data"
                ],
                "destinationTable": {
                    "projectId": f"{project_id}",
                    "datasetId": f"{dataset_id}",
                    "tableId": f"{table_id}"
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        },
        location="US"
    )

# Upload the CSV file to GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.")

source_file_name = "voter_data.csv"
destination_name = "voter_data.csv"

upload_to_gcs(bucket_name, source_file_name, destination_name)

clean_task >> upload_to_gcs