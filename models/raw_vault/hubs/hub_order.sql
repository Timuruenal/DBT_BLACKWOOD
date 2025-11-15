{{
  config(
    materialized='incremental',
    unique_key='order_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  -- Union all sources where order_id appears
  
  -- Source 1: Orders
  SELECT
    order_hk,
    order_id
  FROM {{ ref('stg_orders') }}
  
  UNION ALL
  
  -- Source 2: Order Lines
  SELECT
    order_hk,
    order_id
  FROM {{ ref('stg_order_lines') }}
  
  UNION ALL
  
  -- Source 3: Order Returns
  SELECT
    order_hk,
    order_id
  FROM {{ ref('stg_order_returns') }}
  
  UNION ALL
  
  -- Source 4: Payments
  SELECT
    order_hk,
    order_id
  FROM {{ ref('stg_payments') }}
),

distinct_orders AS (
  SELECT DISTINCT
    order_hk,
    order_id
  FROM source_data
  
  {% if is_incremental() %}
  WHERE order_hk NOT IN (SELECT order_hk FROM {{ this }})
  {% endif %}
)

SELECT
  order_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  order_id
FROM distinct_orders