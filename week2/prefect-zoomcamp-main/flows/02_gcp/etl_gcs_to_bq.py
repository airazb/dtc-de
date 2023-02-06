from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import argparse
import os
from ast import literal_eval


@task(retries=3)
def extract_from_gcs(color: str, year: str, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-de-zoomcamp2023")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Load rows: {len(df)}")
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtc-de-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="ornate-shape-375811",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year, month, color):
    """Main ETL flow to load data into Big Query"""
    
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: str = "2021", color: str = "yellow"):
    rows = 0
    for month in months:
        month = int(month)
        rows += etl_gcs_to_bq(year, month, color)
    print(f"Load total rows: {rows}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL flow to load data into Big Query')

    parser.add_argument('--color', required=True, help='Color of taxicolor')
    parser.add_argument('--year', required=True, help='Year')
    parser.add_argument('--months', required=True, help='Months')
    args = parser.parse_args()
    print(args)

    color = args.color
    year = args.year
    months = literal_eval(args.months)

    etl_parent_flow(months, year, color)
