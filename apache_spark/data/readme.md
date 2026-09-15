
Data Pipeline Lab Exercise Requirements:

Step 1: Data Acquisition

    -Download High Volume For-Hire Vehicle Trip Records (PARQUET)
       Source: https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-06.parquet
       upload to s3: s3://deb-01-silver-layer/transport/bookings/year=2024/month=06/

    -Download Data Dictionary for Trip Records
        Source: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf
    -Download Location Data (CSV)
        Source: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
        refer to same dim_location table as we did in Athena lab exercise
    -Upload exchange rates data to s3: s3://deb-01-silver-layer/exchange_rates_monthly/year=2024/month=06/

Step 2: Data Processing and Transformation

        -Load Raw Data into DataFrames
            *Load the FHVHV trip data and exchange rate data into PySpark DataFrames.

        -Perform Data Transformations
            *Apply schema validation and required transformations to clean and enrich the data.

        -Write Transformed Data to Staging Path
            *Save the transformed data to the staging area in S3 using a partitioned structure (e.g., by year, month, day).

        -Read Back Staged Data
            *Verify the staged data by reading it back into a PySpark DataFrame to ensure it matches expectations.

Step 3: Data Quality Checks and Data Loading

        -Run Data Quality Checks (DQC)
            *Perform data validation checks on the transformed data in the staging path to ensure accuracy, completeness, and consistency.

        -Swap Staging Data to Main Path
            *If all data quality checks pass, move the staged data to the main path (fact_bookings in S3). 
            This step replaces any existing data for overlapping partitions without impacting historical data.

Step 4: Cost Estimate for EMR Cluster 

        -Instance Type: m5.xlarge
        -Configuration:
            *1 Primary Node
            *1 Core Node
            *1 Task Node
        -Hourly Rate: $0.24 per instance
        -Estimated Pipeline Run Time: 5 minutes
        -Total Cost: Approximately $0.06 per run