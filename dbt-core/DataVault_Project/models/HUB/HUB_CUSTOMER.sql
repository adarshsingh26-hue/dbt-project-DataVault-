{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21         Adarsh       Initial Version
-#}

{{ config(
    materialized='incremental'
) }}

WITH source_data AS (
    SELECT 
        customer_id,
        MD5(CONCAT(CAST(customer_id AS STRING), '{{ source('raw', 'raw_customers') }}')) AS customer_key,
        CURRENT_TIMESTAMP() AS load_date,
        '{{ source('raw', 'raw_customers') }}' AS record_source
    FROM 
        {{ source('raw', 'raw_customers') }}
)

SELECT 
    customer_key,
    customer_id,
    load_date,
    record_source
FROM 
    source_data


