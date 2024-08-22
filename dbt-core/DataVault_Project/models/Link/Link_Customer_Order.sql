{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21        Adarsh       Initial version
-#}

{{ config(materialized='incremental') }}

WITH source_data AS (
    SELECT
        o.customer_id,
        o.order_id,
        CURRENT_TIMESTAMP() AS load_dts,
        'R' AS source
    FROM {{ source('R_ORDERS', 'RAW_ORDERS')}} o
)

SELECT
    MD5(CONCAT(customer_id::TEXT, order_id::TEXT)) AS customer_order_hk,
    MD5(customer_id::TEXT) AS customer_hk,
    MD5(order_id::TEXT) AS order_hk,
    load_dts,
    source
FROM source_data

{% if is_incremental() %}

WHERE MD5(CONCAT(customer_id::TEXT, order_id::TEXT)) NOT IN (SELECT customer_order_hk FROM {{ this }})

{% endif %}
