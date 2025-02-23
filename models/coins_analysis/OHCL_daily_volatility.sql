WITH ohlc_volatility AS (
    SELECT 
        ID AS CRYPTO_ID, -- Remplacez CRYPTO_ID par ID pour correspondre à transformed_coingecko_data_v
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        (HIGH_PRICE - LOW_PRICE) / NULLIF(CLOSE_PRICE, 0) * 100 AS DAILY_VOLATILITY_PERCENTAGE
    FROM {{ ref('transformed_coingecko_data_v') }}
),

coin_metadata AS (
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        MARKET_CAP,
        CIRCULATING_SUPPLY,
        TOTAL_SUPPLY,
        ATH,
        ATL,
        CREATION_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
)

SELECT 
    v.CRYPTO_ID,
    m.SYMBOL,
    m.NAME,
    AVG(v.DAILY_VOLATILITY_PERCENTAGE) AS AVG_DAILY_VOLATILITY,
    AVG(NULLIF(m.MARKET_CAP, 0)) AS AVG_MARKET_CAP, -- Gestion des valeurs NULL ou 0
    (m.CIRCULATING_SUPPLY / NULLIF(m.TOTAL_SUPPLY, 0)) * 100 AS SUPPLY_RATIO_PERCENTAGE,
    (m.CLOSE_PRICE - m.ATH) / NULLIF(m.ATH, 0) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
    (m.CLOSE_PRICE - m.ATL) / NULLIF(m.ATL, 0) * 100 AS DISTANCE_TO_ATL_PERCENTAGE
FROM ohlc_volatility v
JOIN coin_metadata m
    ON v.CRYPTO_ID = m.CRYPTO_ID
GROUP BY v.CRYPTO_ID, m.SYMBOL, m.NAME, m.CIRCULATING_SUPPLY, m.TOTAL_SUPPLY, m.ATH, m.ATL, m.CLOSE_PRICE
ORDER BY AVG_DAILY_VOLATILITY DESC
LIMIT 20