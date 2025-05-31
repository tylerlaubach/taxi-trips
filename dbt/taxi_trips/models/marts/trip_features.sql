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
    *
    , (EXTRACT(epoch FROM dropoff_ts) 
        - EXTRACT(epoch FROM pickup_ts)
        ) / 60.0 AS trip_duration_min
    , EXTRACT(hour FROM pickup_ts) AS pickup_hour
    , EXTRACT(dow  FROM pickup_ts) AS pickup_weekday
    , CASE
        WHEN trip_distance_miles < 2 THEN 'short'
        WHEN trip_distance_miles < 5 THEN 'medium'
        ELSE 'long'
        END AS trip_distance_category
    , fare_amount / NULLIF(trip_distance_miles, 0) AS fare_per_mile
    , STRFTIME(pickup_ts, '%Y-%m')  AS year_month
FROM base
