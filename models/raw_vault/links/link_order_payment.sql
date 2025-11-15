{{
  config(
    materialized='incremental',
    unique_key='link_order_payment_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT DISTINCT
    link_order_payment_hk,
    payment_hk,
    order_hk
  FROM {{ ref('stg_payments') }}
  
  {% if is_incremental() %}
  WHERE link_order_payment_hk NOT IN (SELECT link_order_payment_hk FROM {{ this }})
  {% endif %}
)

SELECT
  link_order_payment_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  payment_hk,
  order_hk
FROM source_data