{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21        Adarsh       Initial version
-#}

{{ config(materialized='incremental') }}

WITH orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS order_count,
        SUM(order_amount) AS total_order_amount,
        'R' AS source
    FROM {{ source('R_ORDERS', 'RAW_ORDERS')}}
    GROUP BY customer_id
)

SELECT
    {{ generate_hash_key(['customer_id::TEXT']) }} as customer_hk,,
    order_count,
    total_order_amount,
    CURRENT_TIMESTAMP() AS load_dts,
    'AGG' AS source
FROM orders

{% if is_incremental() %}

WHERE MD5(customer_id::TEXT) NOT IN (SELECT customer_hk FROM {{ this }})

{% endif %}
