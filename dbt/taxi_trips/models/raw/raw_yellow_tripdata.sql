{{ config(materialized='view') }}

select *
from read_parquet('../../data/raw/yellow_tripdata_*.parquet')
