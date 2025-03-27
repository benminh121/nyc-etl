import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
from pyspark.sql.functions import col, concat_ws, sha2, unix_timestamp, lit
from pyspark.sql import types

from pyspark import SparkContext

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "utils"))
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


###############################################
# PySpark
###############################################
def create_spark_session():
    """
    Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder.config("spark.driver.memory", "14g")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "s3a://iceberg/staging")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .getOrCreate()
        )

        logging.info("Spark session successfully created!")

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
    Establish the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", MINIO_ACCESS_KEY
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", MINIO_SECRET_KEY
        )
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.connection.ssl.enabled", "false"
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        logging.info("MinIO configuration is created successfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(
            f"MinIO config could not be created successfully due to exception: {e}"
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_session()
    sc = spark.sparkContext
    load_minio_config(sc)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )

    # Create bucket 'processed'
    client.create_bucket("iceberg")

    tables = ["yellow_tripdata", "green_tripdata"]
    years = ["2019", "2020", "2021", "2022", "2023", "2024"]

    for table in tables:
        for year in years:
            for month in range(1, 12):
                raw_path = (
                    f"s3a://processed/{table}/{table}_{year}-{month:02d}.parquet"
                )
                # Load the data
                df = spark.read.parquet(raw_path)

                if not spark.catalog.tableExists("local.nyc.fact_trip"):
                    df.writeTo("local.nyc.fact_trip").createOrReplace()
                else:
                    df.writeTo("local.nyc.fact_trip").append()

