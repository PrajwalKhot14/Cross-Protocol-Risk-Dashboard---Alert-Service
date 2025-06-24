{{ config(materialized='table') }}

SELECT
  date_trunc('hour', snapshot_time) AS hour,
  avg(delta)    AS avg_delta,
  min(delta)    AS min_delta,
  max(delta)    AS max_delta
FROM {{ ref('risk_deltas') }}
GROUP BY 1
