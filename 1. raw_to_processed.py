import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
from pyspark.sql.functions import col, concat_ws, sha2, lit, year, month
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
            .config("spark.sql.catalog.local.warehouse", "s3a://processed/iceberg")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .master("local[*]")
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
    start_time = time.time()

    spark = create_spark_session()
    load_minio_config(spark.sparkContext)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )

    # Create bucket 'processed'
    client.create_bucket("processed")

    # tables = ["yellow_tripdata", "green_tripdata"]
    # years = ["2019"]

    tables = ["yellow_tripdata", "green_tripdata"]
    years = ["2019", "2020", "2021", "2022", "2023", "2024"]

    for table in tables:
        for y in years:
            for m in range(1, 3):
                # Define paths
                raw_path = f"s3a://raw/{table}/{y}/{table}_{y}-{m:02d}.parquet"
                silver_path = f"{table}_{y}-{m:02d}.parquet"

                print(50 * "-")
                print(f"Processing {table} for y {y}/{m:02d}")

                # Load the raw data
                df = spark.read.parquet(raw_path)
                if table == "yellow_tripdata":
                    df = df.select(
                        # identifiers
                        col("VendorID").cast("long").alias("vendor_id"),
                        col("RatecodeID").cast("int").alias("ratecodeid"),
                        col("PULocationID").cast("int").alias("pickup_locationId"),
                        col("DOLocationID").cast("int").alias("dropoff_locationId"),
                        # timestamps
                        col("tpep_pickup_datetime")
                        .cast("date")
                        .alias("pickup_datetime"),
                        col("tpep_dropoff_datetime")
                        .cast("date")
                        .alias("dropoff_datetime"),
                        # trip info
                        col("passenger_count").cast("double").alias("passenger_count"),
                        col("trip_distance").cast("double").alias("trip_distance"),
                        lit(1).alias("trip_type"),
                        # payment info
                        col("payment_type").cast("int").alias("payment_type"),
                        col("fare_amount").cast("double").alias("fare_amount"),
                        col("extra").cast("double").alias("extra"),
                        col("mta_tax").cast("double").alias("mta_tax"),
                        col("tip_amount").cast("double").alias("tip_amount"),
                        col("tolls_amount").cast("double").alias("tolls_amount"),
                        lit(0.0).alias("ehail_fee"),
                        col("improvement_surcharge")
                        .cast("double")
                        .alias("improvement_surcharge"),
                        col("total_amount").cast("double").alias("total_amount"),
                        col("congestion_surcharge")
                        .cast("double")
                        .alias("congestion_surcharge"),
                        col("airport_fee").cast("double").alias("airport_fee"),
                    )
                    df = df.withColumn("taxi_type", lit("yellow"))

                elif table == "green_tripdata":
                    df = df.select(
                        # identifiers
                        col("VendorID").cast("long").alias("vendor_id"),
                        col("RatecodeID").cast("int").alias("ratecodeid"),
                        col("PULocationID").cast("int").alias("pickup_locationId"),
                        col("DOLocationID").cast("int").alias("dropoff_locationId"),
                        # timestamps
                        col("lpep_pickup_datetime")
                        .cast("date")
                        .alias("pickup_datetime"),
                        col("lpep_dropoff_datetime")
                        .cast("date")
                        .alias("dropoff_datetime"),
                        # trip info
                        col("passenger_count").cast("double").alias("passenger_count"),
                        col("trip_distance").cast("double").alias("trip_distance"),
                        col("trip_type").cast("int").alias("trip_type"),
                        # payment info
                        col("payment_type").cast("int").alias("payment_type"),
                        col("fare_amount").cast("double").alias("fare_amount"),
                        col("extra").cast("double").alias("extra"),
                        col("mta_tax").cast("double").alias("mta_tax"),
                        col("tip_amount").cast("double").alias("tip_amount"),
                        col("tolls_amount").cast("double").alias("tolls_amount"),
                        col("ehail_fee").cast("double").alias("ehail_fee"),
                        col("improvement_surcharge")
                        .cast("double")
                        .alias("improvement_surcharge"),
                        col("total_amount").cast("double").alias("total_amount"),
                        col("congestion_surcharge")
                        .cast("double")
                        .alias("congestion_surcharge"),
                        lit(0.0).alias("airport_fee"),
                    )
                    df = df.withColumn("taxi_type", lit("green"))

                    # Generate surrogate key (trip_id) by hashing VendorID, lpep_pickup_datetime, and taxi_type
                df = df.withColumn(
                    "trip_id",
                    sha2(
                        concat_ws(
                            "_",
                            col("vendor_id"),
                            col("pickup_datetime"),
                            col("taxi_type"),
                        ),
                        256,
                    ),
                )

                # Add pickup year and month columns
                df = df.withColumn("pickup_year", year(col("pickup_datetime")))
                df = df.withColumn("pickup_month", month(col("pickup_datetime")))

                # Filter data to keep only values within the appropriate ranges
                df = df.filter(
                    (col("pickup_locationId").between(1, 265))
                    & (col("dropoff_locationId").between(1, 265))
                    & (col("payment_type").between(1, 5))
                    & (col("passenger_count") > 0)
                    & (col("trip_distance") > 0)
                    & (col("fare_amount") > 0)
                    & (col("total_amount") > 0)
                    & (col("payment_type").between(1, 5))
                    & (col("pickup_year") == y)
                    & (col("pickup_month").between(1, 12))
                )
                # Check catalog of table exists
                if spark.catalog.tableExists(f"local.nyc.{table}"):
                    # Append data to existing table
                    df.writeTo(f"local.nyc.{table}").append()
                else:
                    # Create new table
                    df.writeTo(f"local.nyc.{table}").createOrReplace()
