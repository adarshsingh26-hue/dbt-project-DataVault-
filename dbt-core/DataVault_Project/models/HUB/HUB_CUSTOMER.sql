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
        CURRENT_TIMESTAMP() AS load_dts,
        'R' as source
    FROM {{ source('R_CUSTOMERS', 'RAW_CUSTOMERS')}}
)

SELECT
    {{ generate_hash_key(['customer_id::TEXT']) }} as customer_hk,,
    customer_id,
    load_dts,
    source
FROM source_data

{% if is_incremental() %}

WHERE MD5(customer_id::TEXT) NOT IN (SELECT customer_hk FROM DBT_DB.DBT_HUB.HUB_CUSTOMER)

{% endif %}
