from pyspark.sql import SparkSession

# Initialize Spark Session with Iceberg
spark = SparkSession.builder \
    .appName("Iceberg-Table-Creation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://raw/staging") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "benminh1201") \
    .config("spark.hadoop.fs.s3a.secret.key", "benminh1201") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'
# Create Yello Iceberg Table using PySpark SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.nyc.yellow_tripdata (
        vendorid BIGINT,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        ratecodeid DOUBLE,
        store_and_fwd_flag VARCHAR(255),
        PULocationID BIGINT,
        DOLocationID BIGINT,
        payment_type BIGINT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        airport_fee DOUBLE
    )
""")
#  'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type'
# Create Green Iceberg Table using PySpark SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.nyc.green_tripdata (
        vendorid BIGINT,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag STRING,
        ratecodeid DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type INT,
        trip_type INT,
        congestion_surcharge DOUBLE
    )
""")

# 'dispatching_base_num', 'pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID', 'SR_Flag'
# Create FHV Iceberg Table using PySpark SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.nyc.fhv_tripdata (
        dispatching_base_num STRING,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        sr_flag INT
    )
""")

# Create Zone Lookup Iceberg Table using PySpark SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.nyc.taxi_zone_lookup (
        locationID INT,
        borough STRING,
        zone STRING,
        service_zone STRING
    )
""")
print("✅ Iceberg table created successfully!")