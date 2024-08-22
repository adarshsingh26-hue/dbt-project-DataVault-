{#-

-------------------------------------------------------------------
Revision Date       User          Comment
-------------------------------------------------------------------
2024-08-21        Adarsh       Initial version
-#}

WITH orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS order_count,
        SUM(order_amount) AS total_order_amount
    FROM {{ source('RAW', 'RAW_ORDERS')}}
    GROUP BY customer_id
)

SELECT
    MD5(customer_id::TEXT) AS customer_hk,
    order_count,
    total_order_amount,
    CURRENT_TIMESTAMP() AS load_dts,
    'AGG' AS source
FROM orders
WHERE MD5(customer_id::TEXT) NOT IN (SELECT customer_hk FROM {{ this }})
