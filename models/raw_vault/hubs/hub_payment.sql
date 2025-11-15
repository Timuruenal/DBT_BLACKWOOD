{{
  config(
    materialized='incremental',
    unique_key='payment_hk',
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT DISTINCT
    payment_hk,
    payment_id
  FROM {{ ref('stg_payments') }}
  
  {% if is_incremental() %}
  WHERE payment_hk NOT IN (SELECT payment_hk FROM {{ this }})
  {% endif %}
)

SELECT
  payment_hk,
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  payment_id
FROM source_data