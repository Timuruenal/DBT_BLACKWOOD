WITH parsed_data AS (
  -- Parse VARIANT data into typed columns
  SELECT
    RAW_DATA:product_id::VARCHAR          AS product_id,
    RAW_DATA:timestamp::BIGINT            AS timestamp,
    RAW_DATA:current_price::FLOAT         AS current_price,
    RAW_DATA:sku::VARCHAR                 AS sku,
    RAW_DATA:name::VARCHAR                AS name,
    RAW_DATA:category::VARCHAR            AS category,
    RAW_DATA:collection::VARCHAR          AS collection,
    RAW_DATA:base_price::FLOAT            AS base_price,
    RAW_DATA:launch_date::DATE            AS launch_date,
    RAW_DATA:discontinuation_date::DATE   AS discontinuation_date,
    RAW_DATA:subcategory::VARCHAR         AS subcategory,
    RAW_DATA:description::VARCHAR         AS description,
    RAW_DATA:material::VARCHAR            AS material
    
  FROM {{ source('blackwood', 'products') }}
)

SELECT
  -- Business Key
  product_id,
  
  -- Descriptive Attributes
  {{convert_unix_to_timestamp('timestamp')}} AS timestamp,
  current_price,
  sku,
  name,
  category,
  collection,
  base_price,
  launch_date,
  discontinuation_date,
  subcategory,
  description,
  material,
  
  -- Metadata
  CURRENT_DATE() AS load_date,
  'BLACKWOOD_SHOP' AS record_source,
  
  -- Calculated Hash Keys
  MD5(product_id) AS product_hk,
  
  -- Hash Diff for Satellite
  MD5(CONCAT_WS('||',
    COALESCE(current_price::VARCHAR, '~'),
    COALESCE(sku, '~'),
    COALESCE(name, '~'),
    COALESCE(category, '~'),
    COALESCE(collection, '~'),
    COALESCE(base_price::VARCHAR, '~'),
    COALESCE(launch_date::VARCHAR, '~'),
    COALESCE(discontinuation_date::VARCHAR, '~'),
    COALESCE(subcategory, '~'),
    COALESCE(description, '~'),
    COALESCE(material, '~')
  )) AS hash_diff
  
FROM parsed_data