{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21         Adarsh       Initial version
-#}

{{ config(materialized='incremental') }}

WITH source_data AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        customer_phone,
        CURRENT_TIMESTAMP() AS load_dts,
        'R' AS source
    FROM {{ source('R_CUSTOMERS', 'RAW_CUSTOMERS')}}
)

SELECT
    {{ generate_hash_key(['customer_id::TEXT']) }} as customer_hk,,
    customer_name,
    customer_email,
    customer_phone,
    load_dts,
    source
FROM source_data

{% if is_incremental() %}

WHERE MD5(customer_id::TEXT) NOT IN (SELECT customer_hk FROM {{ this }})

{% endif %}

