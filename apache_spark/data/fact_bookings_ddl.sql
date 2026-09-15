

CREATE EXTERNAL TABLE IF NOT EXISTS gl_transport.fact_bookings(
        booking_date_key int,
        pu_date_key int,
        pu_hour_key int,
        do_date_key int,
        do_hour_key int,
        pu_location_key int,
        do_location_key int,
        platform_key string,
        source_currency string,
        request_datetime timestamp,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        trip_miles double,
        trip_mins double,
        trip_efficiency double,
        speed_mile_per_hour double,
        base_passenger_fare double,
        tolls double,
        bcf double,
        sales_tax double,
        congestion_surcharge double,
        airport_fee double,
        tips double,
        total_fare double,
        total_fare_gbp double,
        total_fare_eur double,
        driver_pay double,
        driver_pay_gbp double,
        driver_pay_eur double,
        gbp_rate double,
        eur_rate double,
        shared_request_flag boolean,
        shared_match_flag boolean,
        wav_request_flag boolean,
        wav_match_flag boolean
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://deb-01-gold-layer/transport/bookings/fact_bookings/';

ALTER TABLE gl_transport.fact_bookings
ADD PARTITION (year='2024', month='06', day='10')
LOCATION 's3://deb-01-gold-layer/transport/bookings/fact_bookings/year=2024/month=06/day=10';


CREATE EXTERNAL TABLE IF NOT EXISTS gl_transport.dim_location (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"'
)
LOCATION 's3://deb-01-gold-layer/transport/bookings/dim_location/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS gl_transport.dim_platform (
    platform_key STRING,
    platform_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"'
)
LOCATION 's3://deb-01-gold-layer/transport/bookings/dim_platform/'
TBLPROPERTIES ('skip.header.line.count' = '1');



ALTER TABLE gl_transport.fact_bookings
ADD PARTITION (year='2024', month='06', day='10')
LOCATION 's3://deb-01-gold-layer/transport/bookings/fact_bookings/year=2024/month=06/day=10';

select * from gl_transport.fact_bookings
where year='2024' and month='06' and day='10'
limit 100;

select * from gl_transport.dim_location ;

select * from gl_transport.dim_platform;

select p.platform_name, count(1)
from gl_transport.fact_bookings f
left join gl_transport.dim_platform p on f.platform_key=p.platform_key
where year='2024' and month='06' and day='10'
group by 1;

