# End-to-end data transformation pipeline

from datetime import datetime, timedelta
import boto3
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import (StructType, StructField, TimestampType, IntegerType,
                               DecimalType, StringType, BooleanType, DoubleType)

logger = logging.getLogger(__name__)

# Constants
C_RAW_BOOKINGS_S3_PATH = "s3://deb-01-silver-layer-lab-590183679875/transport/bookings/"
C_EXCHANGE_RATE_S3_PATH = "s3://deb-01-silver-layer-lab-590183679875/exchange_rates_monthly/"
C_FACT_BOOKINGS_S3_PATH = "s3://deb-01-gold-layer-lab-590183679875/datawarehouse/fact_bookings/"
C_FACT_BOOKINGS_STAGING_S3_PATH = "s3://deb-01-gold-layer-lab-590183679875/datawarehouse/staging_fact_bookings/"
C_GOLD_LAYER_S3_BUCKET = "s3://deb-01-gold-layer-lab-590183679875/"


# Define UDF to calculate trip efficiency
# calculate trip efficiency as the ratio of distance to time
def calculate_trip_efficiency(trip_miles, trip_time):
    if trip_time > 120 and trip_miles > 1:  # Only consider trips over 2 minutes and 1 mile
        return trip_miles / (trip_time / 60)  # Efficiency in miles per minute
    else:
        return None  # Invalid trips have no efficiency


# Register UDF
trip_efficiency_udf = udf(calculate_trip_efficiency, DoubleType())

fact_bookings_schema = StructType(
    [
        StructField("booking_date_key", IntegerType(), False),
        StructField("pu_date_key", IntegerType(), False),
        StructField("pu_hour_key", IntegerType(), False),
        StructField("do_date_key", IntegerType(), False),
        StructField("do_hour_key", IntegerType(), False),
        StructField("pu_location_key", IntegerType(), False),
        StructField("do_location_key", IntegerType(), False),
        StructField("platform_key", StringType(), False),
        StructField("source_currency", StringType(), False),
        StructField("request_datetime", TimestampType(), False),
        StructField("pickup_datetime", TimestampType(), False),
        StructField("dropoff_datetime", TimestampType(), False),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_mins", DoubleType(), True),
        StructField("trip_efficiency", DoubleType(), True),
        StructField("speed_mile_per_hour", DoubleType(), True),
        StructField("base_passenger_fare", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("bcf", DoubleType(), True),
        StructField("sales_tax", DoubleType(), True),
        StructField("congestion_surcharge",DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("total_fare", DoubleType(), True),
        StructField("total_fare_gbp", DoubleType(), True),
        StructField("total_fare_eur", DoubleType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("driver_pay_gbp", DoubleType(), True),
        StructField("driver_pay_eur", DoubleType(), True),
        StructField("gbp_rate", DoubleType(), True),
        StructField("eur_rate", DoubleType(), True),
        StructField("shared_request_flag", BooleanType(), True),
        StructField("shared_match_flag", BooleanType(), True),
        StructField("wav_request_flag", BooleanType(), True),
        StructField("wav_match_flag", BooleanType(), True),
        StructField("year", StringType(), False),
        StructField("month", StringType(), False),
        StructField("day", StringType(), False),
    ]
)


def validate_schema(df, expected_schema):
    """
    Validate the schema of a DataFrame against a given schema definition.

    :param df: DataFrame to validate.
    :param expected_schema: StructType object representing the expected schema.
    :return: Validation results as a dictionary.
    """
    validation_results = {
        "missing_fields": [],
        "extra_fields": [],
        "datatype_mismatches": [],
        "nullability_mismatches": []
    }

    # Extract schema details from DataFrame and expected schema
    df_schema_fields = {field.name: field for field in df.schema.fields}
    expected_schema_fields = {field.name: field for field in expected_schema.fields}

    # Check for missing fields in DataFrame
    for field_name, expected_field in expected_schema_fields.items():
        if field_name not in df_schema_fields:
            validation_results["missing_fields"].append(field_name)
        else:
            # Compare data type
            df_field = df_schema_fields[field_name]
            if not isinstance(df_field.dataType,
                              type(expected_field.dataType)) or df_field.dataType != expected_field.dataType:
                validation_results["datatype_mismatches"].append({
                    "field": field_name,
                    "expected": expected_field.dataType,
                    "actual": df_field.dataType
                })

            # Compare nullability
            if df_field.nullable != expected_field.nullable:
                validation_results["nullability_mismatches"].append({
                    "field": field_name,
                    "expected": expected_field.nullable,
                    "actual": df_field.nullable
                })

    # Check for extra fields in DataFrame
    for field_name in df_schema_fields.keys():
        if field_name not in expected_schema_fields:
            validation_results["extra_fields"].append(field_name)

    return validation_results


def run_dqc(spark: SparkSession, df: DataFrame) -> bool:
    """
    Perform Data Quality Checks (DQC) on the transformed dataset.

    :param spark: Spark Session
    :param intermediate_path: Path to the intermediate S3 bucket
    :return: True if all DQC checks pass, otherwise raise an exception
    """
    logger.info("Running data quality checks.")
    try:
        # Load transformed data
        df.createOrReplaceTempView("transformed_bookings_view")

        # Define DQC SQL Queries
        dqc_queries = {
            "null_check": """
                                SELECT COUNT(*) AS cnt
                                FROM transformed_bookings_view
                                WHERE 
                                    source_currency IS NULL OR
                                    request_datetime IS NULL 
                            """,
            "large_values_check": """
                                SELECT COUNT(*) AS cnt
                                FROM transformed_bookings_view
                                WHERE 
                                    total_fare > 10000 ;
                            """,
            "date_consistency_check": """
                                SELECT COUNT(*) AS cnt
                                FROM transformed_bookings_view
                                WHERE dropoff_datetime < pickup_datetime;
                            """
        }

        # Run each check
        for checker_name, query in dqc_queries.items():
            result = spark.sql(query).collect()[0]['cnt']
            if result > 0:
                raise Exception(f"Data Quality Check Failed: {checker_name} has {result} issues.")

        logger.info("All data quality checks passed successfully.")
        return True
    except Exception as e:
        logger.error(f"Data quality checks failed: {e}")
        raise


def swap_s3_paths(intermediate_path: str, main_path: str):
    """
    Swap S3 intermediate path to main path after DQC checks, preserving historical data.
    Only overwrites partitions found in the intermediate path.

    :param intermediate_path: Intermediate path in S3 (e.g., "s3://my-bucket/intermediate/")
    :param main_path: Main path in S3 (e.g., "s3://my-bucket/main/")
    """
    logger.info("Starting partial swap of intermediate path to main path.")
    s3_client = boto3.client('s3')

    try:
        # Extract bucket name and prefixes
        intermediate_bucket, intermediate_prefix = intermediate_path.replace("s3://", "").split("/", 1)
        main_bucket, main_prefix = main_path.replace("s3://", "").split("/", 1)

        # Step 1: List partitions in the intermediate path
        logger.info(f"Listing partitions in intermediate path: {intermediate_path}")
        intermediate_partitions = list_partitions_in_prefix(s3_client, intermediate_bucket, intermediate_prefix)

        # Step 2: Delete matching partitions in the main path
        for partition in intermediate_partitions:
            partition_prefix = f"{main_prefix}{partition}"
            logger.info(f"Deleting partition in main path: {partition_prefix}")
            delete_objects_in_prefix(s3_client, main_bucket, partition_prefix)

        # Step 3: Copy partitions from intermediate to main path
        logger.info(f"Copying data from {intermediate_path} to {main_path}")
        copy_objects_between_prefixes(s3_client, intermediate_bucket, intermediate_prefix, main_bucket, main_prefix)

        logger.info("Successfully completed partial swap.")
    except Exception as e:
        logger.error(f"Failed to complete partial swap: {e}")
        raise


def list_partitions_in_prefix(s3_client, bucket_name: str, prefix: str) -> set:
    """
    List all partitions under a given S3 prefix.

    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the S3 bucket
    :param prefix: S3 prefix to list objects from
    :return: Set of partition subdirectories (e.g., "year=2024/month=11/day=01/")
    """
    partitions = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Extract partition path (e.g., "year=2024/month=11/day=01/")
                partition_path = "/".join(key[len(prefix):].split("/")[:3]) + "/"
                partitions.add(partition_path)
    logger.info(f"Found partitions: {partitions}")
    return partitions


def delete_objects_in_prefix(s3_client, bucket_name: str, prefix: str):
    """
    Delete all objects under a given S3 prefix.

    :param s3_client: Boto3 S3 client
    :param bucket_name: Name of the S3 bucket
    :param prefix: S3 prefix to delete objects from
    """
    logger.info(f"Deleting objects under prefix: {prefix}")
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_keys})


def copy_objects_between_prefixes(s3_client, source_bucket: str, source_prefix: str, target_bucket: str, target_prefix: str):
    """
    Copy objects from one S3 prefix to another.

    :param s3_client: Boto3 S3 client
    :param source_bucket: Source S3 bucket
    :param source_prefix: Source prefix in the S3 bucket
    :param target_bucket: Target S3 bucket
    :param target_prefix: Target prefix in the S3 bucket
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
                target_key = obj['Key'].replace(source_prefix, target_prefix, 1)
                logger.info(f"Copying {obj['Key']} to {target_key}")
                s3_client.copy_object(Bucket=target_bucket, CopySource=copy_source, Key=target_key)


def load(spark: SparkSession, df: DataFrame):
    # this load function ensures data reliability and quality checks before
    # committing data to the fact_bookings main path
    try:
        # Run Data Quality Checks
        if run_dqc(spark, df):
            # Swap S3 staging path to fact bookings main path
            swap_s3_paths(intermediate_path=C_FACT_BOOKINGS_STAGING_S3_PATH, main_path=C_FACT_BOOKINGS_S3_PATH)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


def transform(spark: SparkSession, date_str: str) -> DataFrame:
    """
    Transform the extracted data.
    :param spark: Spark Session
    :param date_str: Date string in 'YYYY-MM' format
    :return: Transformed DataFrame
    """
    logger.info(f"Transforming data for {date_str}.")
    # year, month = map(int, date_str.split('-'))
    year, month = date_str.split('-')
    # Load raw bookings and exchange_rate data
    raw_bookings_df = spark.read.parquet(C_RAW_BOOKINGS_S3_PATH + f"year={year}/month={month}/")
    exchange_rate_df = spark.read.parquet( C_EXCHANGE_RATE_S3_PATH + f"year={year}/month={month}/")

    raw_bookings_df.createOrReplaceTempView("raw_bookings_view")
    exchange_rate_df.createOrReplaceTempView("ref_exchange_rate_view")

    curated_bookings_df = spark.sql("""
                SELECT 
                    hvfhs_license_num AS platform_key,
                    'USD' AS source_currency,
                    request_datetime,
                    date_format(request_datetime, 'yyyy-MM-dd') AS booking_date_str,
                    date_format(request_datetime, 'yyyy') AS year,
                    date_format(request_datetime, 'MM') AS month,
                    date_format(request_datetime, 'dd') AS day,
                    pickup_datetime,
                    dropoff_datetime, 
                    cast(date_format(request_datetime, 'yyyyMMdd') as int) AS booking_date_key,
                    cast(date_format(pickup_datetime, 'yyyyMMdd') as int) AS pu_date_key,
                    cast(date_format(pickup_datetime, 'yyyyMMddHH') as int) AS pu_hour_key,
                    cast(date_format(dropoff_datetime, 'yyyyMMdd') as int) AS do_date_key,
                    cast(date_format(dropoff_datetime, 'yyyyMMddHH') as int) AS do_hour_key,
                    cast(PULocationID as int) AS pu_location_key,
                    cast(DOLocationID as int) AS do_location_key,
                    trip_miles,
                    trip_time,
                    trip_time / 60 AS trip_mins,
                    trip_miles / (trip_time / 3600) AS speed_mile_per_hour,
                    base_passenger_fare,
                    tolls,
                    bcf,
                    sales_tax,
                    congestion_surcharge,
                    airport_fee,
                    tips,
                    base_passenger_fare +
                        tolls +
                        bcf +
                        sales_tax +
                        congestion_surcharge +
                        airport_fee +
                        tips AS total_fare,
                    driver_pay,
                    cast(shared_request_flag as boolean) as shared_request_flag,
                    cast(shared_match_flag as boolean) as shared_match_flag,
                    cast(wav_request_flag as boolean) as wav_request_flag,
                    cast(wav_match_flag as boolean) as wav_match_flag
                FROM raw_bookings_view
            """)
    # Add UDF column for trip efficiency
    curated_bookings_df = curated_bookings_df.withColumn(
        "trip_efficiency", trip_efficiency_udf(col("trip_miles"), col("trip_time"))
    )
    curated_bookings_df.createOrReplaceTempView("curated_bookings_view")

    transformed_bookings_df = spark.sql("""
                SELECT
                    f.platform_key,
                    f.source_currency,
                    cast(f.request_datetime as timestamp) as request_datetime,
                    f.booking_date_str,
                    f.year,
                    f.month,
                    f.day,
                    cast(f.pickup_datetime as timestamp) as pickup_datetime,
                    cast(f.dropoff_datetime as timestamp) as dropoff_datetime,
                    f.booking_date_key,
                    f.pu_date_key,
                    f.pu_hour_key,
                    f.do_date_key,
                    f.do_hour_key,
                    f.pu_location_key,
                    f.do_location_key,
                    f.trip_miles,
                    f.trip_mins,
                    f.trip_efficiency,
                    f.speed_mile_per_hour,
                    f.base_passenger_fare,
                    f.tolls,
                    f.bcf,
                    f.sales_tax,
                    f.congestion_surcharge,
                    f.airport_fee,
                    f.tips,
                    f.total_fare,
                    f.total_fare * r_gbp.rate as total_fare_gbp,
                    f.total_fare * r_eur.rate as total_fare_eur,
                    f.driver_pay,
                    f.driver_pay * r_gbp.rate as driver_pay_gbp,
                    f.driver_pay * r_eur.rate as driver_pay_eur,
                    r_gbp.rate as gbp_rate,
                    r_eur.rate as eur_rate,
                    f.shared_request_flag,
                    f.shared_match_flag,
                    f.wav_request_flag,
                    f.wav_match_flag
                FROM curated_bookings_view f
                LEFT JOIN ref_exchange_rate_view r_gbp on (r_gbp.from_currency=f.source_currency and r_gbp.to_currency='GBP' 
                                                            and f.booking_date_str=r_gbp.exchange_rate_date)
                LEFT JOIN ref_exchange_rate_view r_eur on (r_eur.from_currency=f.source_currency and r_eur.to_currency='EUR'
                                                            and f.booking_date_str=r_eur.exchange_rate_date)
            """)

    transformed_bookings_df = transformed_bookings_df.select(fact_bookings_schema.fieldNames())
    # validate fact schema
    validation_results = validate_schema(transformed_bookings_df, fact_bookings_schema)
    if any(validation_results.values()):
        logger.info("Schema validation issues found:")
        for issue_type, issues in validation_results.items():
            if issues:
                logger.info(f"{issue_type}: {issues}")
    else:
        logger.info("Schema validation passed!")

    # -- partitions --> year/month/day
    logger.info('write parquet files into staging path=%s' % C_FACT_BOOKINGS_STAGING_S3_PATH)
    transformed_bookings_df.repartition(2) \
        .write \
        .option('maxRecordsPerFile', 200000) \
        .option('schema', fact_bookings_schema) \
        .mode('overwrite') \
        .partitionBy('year', 'month', 'day') \
        .parquet(C_FACT_BOOKINGS_STAGING_S3_PATH)

    logger.info("reading transformed data from staging path= %s" % C_FACT_BOOKINGS_STAGING_S3_PATH)
    df_final = spark.read.load(path=C_FACT_BOOKINGS_STAGING_S3_PATH,
                               format="parquet",
                               inferSchema="true",
                               basePath=C_FACT_BOOKINGS_STAGING_S3_PATH)

    return df_final


def main(date_str: str):
    """
    Main function to execute the ETL pipeline.

    :param date_str: Date string in 'YYYY-MM' format
    """
    logger.info(f"Starting FACT BOOKINGS ETL process for date {date_str}.")
    spark = None
    try:
        # .config("spark.driver.memory", "10g") \
        # .config("spark.executor.memory", "10g") \
        spark = SparkSession.builder \
            .appName("FACT BOOKINGS ETL") \
            .getOrCreate()

        # Extract
        # assuming data ingestion pipelines have ingested raw data from source system

        # Transform
        transformed_df = transform(spark, date_str)

        # Load
        load(spark, transformed_df)

        logger.info("FACT BOOKINGS ETL process completed successfully.")
    except Exception as e:
        logger.error(f"FACT BOOKINGS ETL process failed: {e}")
    finally:
        if spark:
            spark.stop()


# Start Data Pipeline
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FACT BOOKINGS ETL")
    parser.add_argument("--date_str", required=True, help="Date string in 'YYYY-MM' format")
    args = parser.parse_args()
    main(date_str=args.date_str)
    # main(date_str="2024-06")

