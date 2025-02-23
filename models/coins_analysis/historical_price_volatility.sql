WITH latest_date AS (
    SELECT MAX(CREATION_DATE) AS LATEST_CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
),
filtered_data AS (
    SELECT *
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE CREATION_DATE = (SELECT LATEST_CREATION_DATE FROM latest_date)
),
volatility_calculation AS (
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        (HIGH_24H - LOW_24H) / NULLIF(CURRENT_PRICE, 0) * 100 AS VOLATILITY_PERCENTAGE
    FROM filtered_data
)
SELECT 
    CRYPTO_ID,
    SYMBOL,
    NAME,
    AVG(VOLATILITY_PERCENTAGE) AS AVG_VOLATILITY
FROM volatility_calculation
GROUP BY CRYPTO_ID, SYMBOL, NAME
ORDER BY AVG_VOLATILITY DESC
LIMIT 20