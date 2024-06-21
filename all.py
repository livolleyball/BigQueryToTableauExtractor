# -*- coding: utf-8 -*-
import argparse
import configparser
import logging
import os
import shutil
import time

import tableauserverclient as TSC
from google.cloud import bigquery
from google.cloud import storage

# --- START YOUR CONFIGURATIONS HERE --- #
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = r"your_google_credentials_file.json"

parser = argparse.ArgumentParser()
parser.add_argument('--config_file', help='config_file')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.config_file, encoding='utf-8')

project = config.get('bigquery', 'project')
dataset_id = config.get('bigquery', 'dataset_id')
table_id = config.get('bigquery', 'table_id')

bucket_name = config.get('google_cloud_storage', 'bucket_name')
location = config.get('google_cloud_storage', 'location')
filename_pattern = config.get('google_cloud_storage', 'filename_pattern')

tableau_url = config.get('tableau_server', 'tableau_url')
tableau_project_name = config.get('tableau_server', 'tableau_project_name')
tableau_workbook_name = config.get('tableau_server', 'tableau_workbook_name')
destination_dir = config.get('tableau_server', 'destination_dir')

logging_level = getattr(logging, "DEBUG")
logging.basicConfig(level=logging_level)

storage_client = storage.Client()


def delete_blob_if_exists(bucket_name, source_folder):

    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=source_folder)
    for blob in blobs:
        blob.delete()
        print(f"Deleted {blob.name} from {bucket_name}")


def export_to_bucket():
    print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    client = bigquery.Client(project=project)
    destination_uri = "gs://{}/{}/{}/{}".format(bucket_name, dataset_id, table_id, filename_pattern)
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    ##  clear path before load data
    delete_blob_if_exists(bucket_name, "{}/{}/".format(dataset_id, table_id))
    print("Data Export from BigQuery to bucket has started...")
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location=location,
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )


def download_blob(bucket_name, source_folder, destination_dir):

    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=source_folder)
    directory_path = os.path.join(destination_dir, source_folder)
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
    for blob in blobs:
        print(blob.name)
        local_path = os.path.join(destination_dir, blob.name)

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        blob.download_to_filename(local_path)
        print(f"Blob {blob.name} downloaded to {local_path}.")


def refresh_workbook_data():
    # SIGN IN
    tableau_auth = TSC.TableauAuth("tableau_user", "tableau_pwd")
    server = TSC.Server(tableau_url, use_server_version=True)

    print(server.use_server_version())
    server.use_highest_version()
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName,
                                     TSC.RequestOptions.Operator.Equals,
                                     tableau_project_name)
                          )
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     tableau_workbook_name)
                          )

    with server.auth.sign_in(tableau_auth):
        all_workbooks, pagination_item = server.workbooks.get(req_option)
        wb = all_workbooks[0]
        logging.info("current workbook is  {}".format(wb.name))
        job = server.workbooks.refresh(wb)
        logging.info("extra task job_id:{}".format(job.id))
        server.jobs.wait_for_job(job.id, timeout=600)


if __name__ == '__main__':
    print("----start-------")
    start_time = time.time()
    export_to_bucket()
    source_folder = "{}/{}/".format(dataset_id, table_id)
    download_blob(bucket_name, source_folder, destination_dir)
    refresh_workbook_data()
    print("the time costs ",time.time() - start_time)
