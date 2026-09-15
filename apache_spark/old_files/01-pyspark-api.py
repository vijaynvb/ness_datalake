# PySpark APIs hands-on

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, hour, desc

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Taxi Data Analysis") \
    .getOrCreate()


# Load raw bookings data
raw_bookings = spark.read.parquet("data/green_tripdata_2024-05.parquet")

# Load location dimension data
dim_location = spark.read.csv("data/dim_location.csv", header=True, inferSchema=True)

# Register as temporary views for SQL queries
raw_bookings.createOrReplaceTempView("raw_bookings")
dim_location.createOrReplaceTempView("dim_location")

# Objective: Find the total trips and revenue by borough for the first week of May 2024.

# Using DataFrame API:

# Filter, join, group, and aggregate
result_df = raw_bookings.filter(
    (col("lpep_dropoff_datetime").between("2024-05-01", "2024-05-07")) &
    (col("total_amount") > 0)
).join(
    dim_location, raw_bookings["DOLocationID"] == dim_location["LocationID"]
).groupBy("borough").agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue")
).orderBy(col("total_revenue").desc())

# Show results
result_df.show()

# Using Spark SQL API:
result_sql = spark.sql("""
    SELECT 
        loc.borough AS borough,
        COUNT(*) AS total_trips,
        SUM(b.total_amount) AS total_revenue
    FROM raw_bookings b
    JOIN dim_location loc ON b.DOLocationID = loc.LocationID
    WHERE b.lpep_dropoff_datetime BETWEEN '2024-05-01' AND '2024-05-07'
        AND b.total_amount > 0
    GROUP BY loc.borough
    ORDER BY total_revenue DESC
""")
result_sql.show()

# Objective: Find the top 5 zones with the highest trip counts and analyze hourly pickup trends.

# Using DataFrame API:

# Step 1: Find the top 5 zones
top_zones = raw_bookings.join(
    dim_location, raw_bookings["PULocationID"] == dim_location["LocationID"]
).groupBy("zone").agg(
    count("*").alias("trip_count")
).orderBy(col("trip_count").desc()).limit(5)

# Step 2: Analyze pickup times for these zones
pickup_times = raw_bookings.join(
    dim_location, raw_bookings["PULocationID"] == dim_location["LocationID"]
).join(
    top_zones, "zone"
).groupBy("zone", hour("lpep_pickup_datetime").alias("pickup_hour")).agg(
    count("*").alias("trips_during_hour")
).orderBy("zone", desc("trips_during_hour"))

# Show results
pickup_times.show()

# Using Spark SQL API:

result_sql = spark.sql("""
    WITH zone_trip_counts AS (
        SELECT 
            loc.zone AS zone,
            COUNT(*) AS trip_count
        FROM raw_bookings b
        JOIN dim_location loc ON b.PULocationID = loc.LocationID
        GROUP BY loc.zone
        ORDER BY trip_count DESC
        LIMIT 5
    )
    SELECT 
        z.zone,
        HOUR(b.lpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS trips_during_hour
    FROM raw_bookings b
    JOIN dim_location loc ON b.PULocationID = loc.LocationID
    JOIN zone_trip_counts z ON loc.zone = z.zone
    GROUP BY z.zone, HOUR(b.lpep_pickup_datetime)
    ORDER BY z.zone, trips_during_hour DESC
""")
result_sql.show()

# Objective: Calculate the average tip percentage by borough for meaningful trips.

# Using DataFrame API:

# Calculate average tip percentage
avg_tip_df = raw_bookings.filter(
    (col("trip_distance") > 2) & (col("total_amount") > 0) & (col("tip_amount") >= 0)
).join(
    dim_location, raw_bookings["PULocationID"] == dim_location["LocationID"]
).groupBy("borough").agg(
    avg((col("tip_amount") / col("total_amount")) * 100).alias("avg_tip_percentage")
).orderBy(col("avg_tip_percentage").desc())

avg_tip_df.show()

# Using Spark SQL API:

result_sql = spark.sql("""
    SELECT 
        loc.borough AS borough,
        AVG((b.tip_amount / b.total_amount) * 100) AS avg_tip_percentage
    FROM raw_bookings b
    JOIN dim_location loc ON b.PULocationID = loc.LocationID
    WHERE  b.trip_distance > 2
        AND b.total_amount > 0
        AND b.tip_amount >= 0
    GROUP BY loc.borough
    ORDER BY avg_tip_percentage DESC
""")
result_sql.show()



