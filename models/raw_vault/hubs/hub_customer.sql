{{
  config(
    materialized='incremental',
    unique_key='customer_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  -- Union all sources where customer_id appears
  
  -- Source 1: Customers
  SELECT
    customer_hk,
    customer_id
  FROM {{ ref('stg_customers') }}
  
  UNION ALL
  
  -- Source 2: Orders
  SELECT
    customer_hk,
    customer_id
  FROM {{ ref('stg_orders') }}
),

distinct_customers AS (
  SELECT DISTINCT
    customer_hk,
    customer_id
  FROM source_data
  
  {% if is_incremental() %}
  WHERE customer_hk NOT IN (SELECT customer_hk FROM {{ this }})
  {% endif %}
)

SELECT
  customer_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  customer_id
FROM distinct_customers