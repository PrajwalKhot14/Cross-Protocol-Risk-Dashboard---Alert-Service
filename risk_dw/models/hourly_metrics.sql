{{ config(materialized='table') }}

SELECT
  date_trunc('hour', snapshot_time) AS hour,
  avg(var)    AS avg_var,
  min(var)    AS min_var,
  max(var)    AS max_var,
  avg(lar)    AS avg_lar
FROM {{ ref('risk_metrics') }}
GROUP BY 1
