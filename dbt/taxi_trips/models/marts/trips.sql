{{ 
    config(
      materialized = 'view'
    ) 
}}

WITH base AS (
    SELECT
         *
    FROM {{ ref('stg_yellow_taxi') }}
)

SELECT
    vendor_id
    , pickup_ts
    , dropoff_ts
    , passenger_count
    , trip_distance_miles
    , rate_code_id
    , store_and_fwd_flag
    , pickup_location_id
    , dropoff_location_id
    , payment_type
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , improvement_surcharge
    , total_amount
    , congestion_surcharge
    , airport_fee
    , (EXTRACT(EPOCH FROM dropoff_ts) 
        - EXTRACT(EPOCH FROM pickup_ts)
        ) / 60.0 AS trip_duration_minutes
    , EXTRACT(HOUR FROM pickup_ts) AS pickup_hour
    , EXTRACT(DOW  FROM pickup_ts) AS pickup_weekday
    , CASE
        WHEN trip_distance_miles < 2 THEN 'short'
        WHEN trip_distance_miles < 5 THEN 'medium'
        ELSE 'long'
        END AS trip_distance_category
    , fare_amount / NULLIF(trip_distance_miles, 0) AS fare_per_mile
    , STRFTIME(pickup_ts, '%Y-%m')  AS year_month
FROM base
