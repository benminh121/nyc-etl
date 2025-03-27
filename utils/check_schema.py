from pyspark.sql import SparkSession
import os
import sys
import pandas as pd

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "utils"))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
###############################################

# Initialize Spark Session with Iceberg
spark = (
    SparkSession.builder.appName("Check Schema")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

taxi_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]

for type in taxi_types:
    # Define MiniO path
    minio_path = f"s3a://raw/{type}/"

    # List all years and months
    years = ["2019", "2020", "2021", "2022", "2023", "2024"]

    months = [f"{i:02d}" for i in range(1, 13)]

    # Create an empty list to store metadata
    metadata_df = pd.DataFrame()

    # Loop through each year and each month's file
    for year in years:
        for month in months:
            file_path = os.path.join(minio_path, year, f"{type}_{year}-{month}.parquet")

            try:
                # Read Parquet file
                df = spark.read.parquet(file_path, mergeSchema=True)

                # Get metadata
                file_name = f"yellow_tripdata_{year}-{month}.parquet"
                columns = df.dtypes
                total_columns = len(columns)

                dict1 = {}
                dict1["file_name"] = file_name
                dict1["total_columns"] = total_columns

                columns = dict(columns)

                # Create metadata row
                metadata_row = dict1 | columns
                temp_df = pd.DataFrame([metadata_row])
                # Append to metadata list
                metadata_df = pd.concat([metadata_df, temp_df], ignore_index=True)

            except Exception as e:
                print(f"⚠️ Skipping {file_path} (File may not exist): {str(e)}")

    # Create bucket
    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )

    client.create_bucket("metadata")

    # Save metadata as CSV file
    output_csv_path = f"s3a://metadata/schema/{type}_tracking_schema.csv"
    sparkDF=spark.createDataFrame(metadata_df)
    sparkDF.write.csv(output_csv_path, header=True, mode="overwrite")


print("✅ Metadata tracking file saved as CSV!")