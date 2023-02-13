from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import argparse
import os


@task(log_prints=True)
def load_from_web(dataset_url: str, file_name: str):
    # download it using requests via a pandas df
    os.system(f"wget {dataset_url} -O {file_name}")
    print(f"Local: {file_name}")

@task(log_prints=True)
def upload_to_gcs(file_name):
    gcs_block = GcsBucket.load("gcs-de-zoomcamp2023")
    gcs_block.upload_from_path(from_path=file_name, to_path=file_name)

@flow()
def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        outdir = Path(f"data/{service}/")
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        dataset_file = f"{service}_tripdata_{year}-{month}.csv.gz"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}"
        file_name = Path(f"data/{service}/{dataset_file}")

        # download file from url 
        load_from_web(dataset_url, file_name)

        # upload it to gcs 
        upload_to_gcs(file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy file from web to GCS')

    parser.add_argument('--year', required=True, help='Year')
    parser.add_argument('--service', required=True, help='Service')
    args = parser.parse_args()
    print(args)
    year = args.year
    service = args.service

    web_to_gcs(year,service)
