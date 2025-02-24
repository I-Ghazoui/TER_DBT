WITH sales_data AS (
    SELECT
        nft_collection,
        COALESCE(NFT_NAME, nft_collection) AS NFT_NAME,
        NFT_IMAGE_URL,
        SALE_PRICE AS price,
        NFT_TOTAL_SALES,
        event_timestamp,

        MIN(sale_price) OVER (PARTITION BY nft_collection) AS floor_price,

        COUNT(DISTINCT nft_identifier) OVER (PARTITION BY nft_collection) AS supply,

        SUM(sale_price) OVER (
            PARTITION BY nft_collection
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND CURRENT ROW
        ) AS "1d_vol",

        (sale_price - LAG(sale_price, 1) OVER (PARTITION BY nft_collection ORDER BY event_timestamp)) AS "1d_changes",

        SUM(sale_price) OVER (
            PARTITION BY nft_collection
            ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL '7 DAY' PRECEDING AND CURRENT ROW
        ) AS "7d_vol",

        (sale_price - LAG(sale_price, 7) OVER (PARTITION BY nft_collection ORDER BY event_timestamp)) AS "7d_changes",

        RANK() OVER (PARTITION BY nft_collection ORDER BY price DESC) AS row_num

    FROM TER_ANALYSIS_DATA.FACT_SALES
    WHERE event_timestamp >= DATEADD(DAY, -7, CURRENT_DATE)
)

SELECT *
FROM sales_data
WHERE row_num = 1
GROUP BY nft_collection,NFT_NAME
ORDER BY price DESC
LIMIT 10
