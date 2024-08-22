{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21         Adarsh       Initial version
-#}

WITH source_data AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        customer_phone,
        CURRENT_TIMESTAMP() AS load_dts,
        'RAW' AS source
    FROM {{ source('RAW', 'RAW_CUSTOMERS')}}
)

SELECT
    MD5(customer_id::TEXT) AS customer_hk,
    customer_name,
    customer_email,
    customer_phone,
    load_dts,
    source
FROM source_data
WHERE MD5(customer_id::TEXT) NOT IN (SELECT customer_hk FROM {{ this }})
