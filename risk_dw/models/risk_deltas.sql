{{ config(materialized='view') }}

SELECT
  to_timestamp(ts)    AS snapshot_time,
  value,
  delta
FROM parquet_scan('../snapshots/risk-deltas-*.parquet')
