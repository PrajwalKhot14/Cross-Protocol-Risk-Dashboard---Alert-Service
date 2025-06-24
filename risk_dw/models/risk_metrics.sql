{{ config(materialized='view') }}

SELECT
  to_timestamp(ts)    AS snapshot_time,
  var,
  lar
FROM parquet_scan('../snapshots/risk-metrics-*.parquet')
