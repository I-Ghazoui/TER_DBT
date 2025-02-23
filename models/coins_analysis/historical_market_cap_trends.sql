WITH latest_date AS (
    SELECT MAX(CREATION_DATE) AS LATEST_CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
),
filtered_data AS (
    SELECT *
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE CREATION_DATE = (SELECT LATEST_CREATION_DATE FROM latest_date)
)
SELECT 
    ID AS CRYPTO_ID,
    SYMBOL,
    NAME,
    MARKET_CAP,
    CREATION_DATE
FROM filtered_data
ORDER BY MARKET_CAP DESC
LIMIT 20