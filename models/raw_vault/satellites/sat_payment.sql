{{
  config(
    materialized='incremental',
    unique_key=['payment_hk', 'load_date'],
    on_schema_change='fail'
  )
}}

WITH source_data AS (
  SELECT
    payment_hk,
    load_date,
    timestamp,
    payment_method,
    payment_status,
    amount,
    transaction_reference,
    retry_count,
    failure_reason,
    hash_diff,
    record_source
  FROM {{ ref('stg_payments') }}
  
  {% if is_incremental() %}
  -- INSERT-only: Füge nur neue Records hinzu (neue hash_diff oder neues load_date)
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} sat
    WHERE sat.payment_hk = {{ ref('stg_payments') }}.payment_hk
      AND sat.hash_diff = {{ ref('stg_payments') }}.hash_diff
  )
  {% endif %}
)

SELECT
  payment_hk,
  load_date,
  timestamp,
  payment_method,
  payment_status,
  amount,
  transaction_reference,
  retry_count,
  failure_reason,
  hash_diff,
  record_source
FROM source_data
