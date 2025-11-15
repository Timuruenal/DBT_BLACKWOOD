{{
  config(
    materialized='incremental',
    unique_key=['customer_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    customer_hk,
    load_date,
    timestamp,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    birth_date,
    signup_date,
    tier,
    status,
    last_purchase_date,
    hash_diff,
    record_source
  FROM {{ ref('stg_customers') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.customer_hk = {{ ref('stg_customers') }}.customer_hk
      AND sat.hash_diff = {{ ref('stg_customers') }}.hash_diff
  )
  {% endif %}
)

SELECT
  customer_hk,
  load_date,
  timestamp,
  first_name,
  last_name,
  email,
  phone_number,
  address,
  birth_date,
  signup_date,
  tier,
  status,
  last_purchase_date,
  hash_diff,
  record_source
FROM source_data
