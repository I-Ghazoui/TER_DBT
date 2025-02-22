{{ config(
    materialized='incremental',
    unique_key='event_hash',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['nft_contract']
) }}

WITH base AS (
    SELECT
        MD5(CONCAT(TRANSACTION, NFT_IDENTIFIER, EVENT_TIMESTAMP)) AS event_hash,
        EVENT_TYPE,
        CHAIN,
        TRANSACTION AS transaction_hash,
        CAST(EVENT_TIMESTAMP AS TIMESTAMP_NTZ) AS event_timestamp,
        NFT_IDENTIFIER,
        NFT_COLLECTION,
        NFT_CONTRACT,
        QUANTITY,
        COALESCE(
            TRY_CAST(PAYMENT_QUANTITY AS DECIMAL(38, 0)) / POWER(10, PAYMENT_DECIMALS),
            0
        ) AS sale_amount,
        PAYMENT_SYMBOL AS currency,
        SELLER,
        BUYER,
        FROM_ADDRESS,
        TO_ADDRESS,
        CAST(CLOSING_DATE AS TIMESTAMP_NTZ) AS closing_date,
    FROM NFT_EVENTS
    WHERE EVENT_TYPE IN ('sale', 'transfer')
)

SELECT
    *
FROM base

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
