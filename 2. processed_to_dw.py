import sys
import os
import traceback
import logging
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

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
            .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg")
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


def create_dim_location_table(spark):
    # Define the CSV file path in the Lakehouse (S3)
    csv_path = "s3a://raw/taxi_zone/taxi_zone_lookup.csv"

    # Define the schema for the CSV file
    schema = StructType(
        [
            StructField("LocationID", IntegerType(), True),
            StructField("Borough", StringType(), True),
            StructField("Zone", StringType(), True),
            StructField("service_zone", StringType(), True),
        ]
    )

    # Load the CSV file into a DataFrame
    dim_location_df = (
        spark.read.format("csv").schema(schema).option("header", "true").load(csv_path)
    )

    print(dim_location_df.show(5))
    # Rename columns to match naming conventions
    dim_location_df = (
        dim_location_df.withColumnRenamed("LocationID", "locationId")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
    )

    # Save the DataFrame as Iceberg table in the Gold layer
    dim_location_df.writeTo("local.nyc.dim_location").createOrReplace()


def create_dim_paymenttype_table(spark):
    # Prepare the data
    data = [
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ]

    # Define the schema
    schema = StructType(
        [
            StructField("payment_type", IntegerType(), True),
            StructField("description", StringType(), True),
        ]
    )

    # Create the DataFrame
    dim_paymenttype_df = spark.createDataFrame(data, schema)

    # Save the DataFrame as a Delta table in the Gold layer
    dim_paymenttype_df.writeTo("local.nyc.dim_paymentType").createOrReplace()


def create_dim_ratecode_table(spark):
    # Prepare the data
    data = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Negotiated fare"),
        (5, "Group ride"),
    ]

    # Define the schema
    schema = StructType(
        [
            StructField("rate_code", IntegerType(), True),
            StructField("description", StringType(), True),
        ]
    )

    # Create the DataFrame
    dim_ratecode_df = spark.createDataFrame(data, schema)

    # Save the DataFrame as a Delta table in the Gold layer
    dim_ratecode_df.writeTo("local.nyc.dim_rateCode").createOrReplace()


def create_dim_date_table(spark):
    startdate = datetime.strptime("2019-01-01", "%Y-%m-%d")
    enddate = datetime.strptime("2025-01-01", "%Y-%m-%d")

    column_rule_df = spark.createDataFrame(
        [
            ("DateID", "cast(date_format(date, 'yyyyMMdd') as int)"),  # 20230101
            ("Year", "year(date)"),  # 2023
            ("Quarter", "quarter(date)"),  # 1
            ("Month", "month(date)"),  # 1
            ("Day", "day(date)"),  # 1
            ("Week", "weekofyear(date)"),  # 1
            ("QuarterNameShort", "date_format(date, 'QQQ')"),  # Q1
            ("QuarterNumberString", "date_format(date, 'QQ')"),  # 01
            ("MonthNameLong", "date_format(date, 'MMMM')"),  # January
            ("MonthNameShort", "date_format(date, 'MMM')"),  # Jan
            ("MonthNumberString", "date_format(date, 'MM')"),  # 01
            ("DayNumberString", "date_format(date, 'dd')"),  # 01
            (
                "WeekNameLong",
                "concat('week', lpad(weekofyear(date), 2, '0'))",
            ),  # week 01
            ("WeekNameShort", "concat('w', lpad(weekofyear(date), 2, '0'))"),  # w01
            ("WeekNumberString", "lpad(weekofyear(date), 2, '0')"),  # 01
            ("DayOfWeek", "dayofweek(date)"),  # 1
            ("YearMonthString", "date_format(date, 'yyyy/MM')"),  # 2023/01
            ("DayOfWeekNameLong", "date_format(date, 'EEEE')"),  # Sunday
            ("DayOfWeekNameShort", "date_format(date, 'EEE')"),  # Sun
            ("DayOfMonth", "cast(date_format(date, 'd') as int)"),  # 1
            ("DayOfYear", "cast(date_format(date, 'D') as int)"),  # 1
        ],
        ["new_column_name", "expression"],
    )

    start = int(startdate.timestamp())
    stop = int(enddate.timestamp())
    df = spark.range(start, stop, 60 * 60 * 24).select(
        col("id").cast("timestamp").cast("date").alias("Date")
    )

    for row in column_rule_df.collect():
        new_column_name = row["new_column_name"]
        expression = expr(row["expression"])
        df = df.withColumn(new_column_name, expression)

    # Save the DataFrame as a Delta table in the Gold layer
    df.writeTo("local.nyc.dim_date").createOrReplace()


def create_fact_trip_table(spark):
    yellow = spark.read.format("parquet").load(
        "s3a://processed/iceberg/nyc/yellow_tripdata/data/"
    )
    green = spark.read.format("parquet").load(
        "s3a://processed/iceberg/nyc/green_tripdata/data/"
    )

    trip_df = yellow.union(green)
    # create datekey
    trip_df = trip_df.withColumn(
        "pickupDateId", date_format(col("pickup_datetime"), "yyyyMMdd")
    ).withColumn("dropoffDateId", date_format(col("dropoff_datetime"), "yyyyMMdd"))

    trip_df = (
        trip_df.drop("pickup_datetime")
        .drop("dropoff_datetime")
        .drop("pickup_year")
        .drop("pickup_month")
    )

    trip_df.writeTo("local.nyc.fact_trip").createOrReplace()


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
    client.create_bucket("warehouse")

    # dim tables
    create_dim_paymenttype_table(spark)
    create_dim_ratecode_table(spark)
    create_dim_location_table(spark)
    create_dim_date_table(spark)
    # fact table
    create_fact_trip_table(spark)
