{% set source_tbl = ref('raw_yellow_tripdata') %}

WITH source AS (
    SELECT
        *
    FROM {{ source_tbl }}
)

, renamed AS (
    SELECT
        CAST(VendorID                AS INTEGER)   AS vendor_id
      , CAST(tpep_pickup_datetime    AS TIMESTAMP) AS pickup_ts
      , CAST(tpep_dropoff_datetime   AS TIMESTAMP) AS dropoff_ts
      , CAST(passenger_count         AS INTEGER)   AS passenger_count
      , CAST(trip_distance           AS DOUBLE)    AS trip_distance_miles
      , CAST(RatecodeID              AS INTEGER)   AS rate_code_id
      , CAST(store_and_fwd_flag      AS VARCHAR)   AS store_and_fwd_flag
      , CAST(PULocationID            AS INTEGER)   AS pickup_location_id
      , CAST(DOLocationID            AS INTEGER)   AS dropoff_location_id
      , CAST(payment_type            AS INTEGER)   AS payment_type
      , CAST(fare_amount             AS DOUBLE)    AS fare_amount
      , CAST(extra                   AS DOUBLE)    AS extra
      , CAST(mta_tax                 AS DOUBLE)    AS mta_tax
      , CAST(tip_amount              AS DOUBLE)    AS tip_amount
      , CAST(tolls_amount            AS DOUBLE)    AS tolls_amount
      , CAST(improvement_surcharge   AS DOUBLE)    AS improvement_surcharge
      , CAST(total_amount            AS DOUBLE)    AS total_amount
      , CAST(congestion_surcharge    AS DOUBLE)    AS congestion_surcharge
      , CAST(Airport_fee             AS DOUBLE)    AS airport_fee
    FROM source
)

SELECT
    *
FROM renamed
