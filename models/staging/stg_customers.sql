WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:customer_id::VARCHAR         AS customer_id,
    RAW_DATA:timestamp::BIGINT            AS timestamp,
    RAW_DATA:first_name::VARCHAR          AS first_name,
    RAW_DATA:last_name::VARCHAR           AS last_name,
    RAW_DATA:email::VARCHAR               AS email,
    RAW_DATA:phone_number::VARCHAR        AS phone_number,
    RAW_DATA:address::VARCHAR             AS address,
    RAW_DATA:birth_date::DATE             AS birth_date,
    RAW_DATA:signup_date::DATE            AS signup_date,
    RAW_DATA:tier::VARCHAR                AS tier,
    RAW_DATA:status::VARCHAR              AS status,
    RAW_DATA:last_purchase_date::DATE     AS last_purchase_date

  FROM {{ source('blackwood', 'customers') }}
)

SELECT
  -- Business Key & Descriptive Attributes
  customer_id,
  {{convert_unix_to_timestamp('timestamp')}} AS timestamp,
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
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Hub Hash Key: MD5 of business key
  MD5(customer_id) AS customer_hk,
  
  -- Hash Diff: MD5 of all descriptive attributes concatenated
  -- Used by Satellite to detect changes in descriptive attributes
  MD5(CONCAT_WS('||',
    COALESCE(first_name, '~'),
    COALESCE(last_name, '~'),
    COALESCE(email, '~'),
    COALESCE(phone_number, '~'),
    COALESCE(address, '~'),
    COALESCE(birth_date::VARCHAR, '~'),
    COALESCE(signup_date::VARCHAR, '~'),
    COALESCE(tier, '~'),
    COALESCE(status, '~'),
    COALESCE(last_purchase_date::VARCHAR, '~')
  )) AS hash_diff
  
FROM parsed_data