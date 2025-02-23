{{
    config(
        partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
        cluster_by=['nft_contract', 'from_address']
    )
}}

WITH transfer_data AS (
    SELECT
        event_hash AS transfer_id,
        event_timestamp,
        nft_collection,
        nft_identifier,
        nft_contract,
        from_address,
        to_address,
        chain,
        COALESCE(TRY_CAST(quantity AS INTEGER), 1) AS quantity,
        COUNT(*) OVER (PARTITION BY nft_identifier, nft_contract) AS total_transfers,
        RANK() OVER (PARTITION BY nft_identifier, nft_contract ORDER BY event_timestamp) AS transfer_rank,
        LAG(to_address) OVER (PARTITION BY nft_identifier, nft_contract ORDER BY event_timestamp) AS prev_owner,
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'transfer'
)

SELECT
    *,
    CASE
        WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 'Burn'
        ELSE 'Transfer'
    END AS transfer_type
FROM transfer_data
