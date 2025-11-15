{{
  config(
    materialized='incremental',
    unique_key='link_customer_order_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT DISTINCT
    link_customer_order_hk,
    customer_hk,
    order_hk
  FROM {{ ref('stg_orders') }}
  
  {% if is_incremental() %}
  WHERE link_customer_order_hk NOT IN (SELECT link_customer_order_hk FROM {{ this }})
  {% endif %}
)

SELECT
  link_customer_order_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  customer_hk,
  order_hk
FROM source_data