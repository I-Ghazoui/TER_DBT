WITH price_trends AS (
    SELECT 
        ID AS CRYPTO_ID,
        TIMESTAMP::DATE AS TRADE_DATE, -- Remplacez OHLC_TIMESTAMP par TIMESTAMP
        CLOSE_PRICE,
        AVG(CLOSE_PRICE) OVER (PARTITION BY ID ORDER BY TIMESTAMP ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(CLOSE_PRICE) OVER (PARTITION BY ID ORDER BY TIMESTAMP ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM {{ ref('transformed_coingecko_data_v') }}
),
trend_analysis AS (
    SELECT 
        p.CRYPTO_ID,
        c.SYMBOL,
        c.NAME,
        p.TRADE_DATE,
        p.CLOSE_PRICE,
        p.MA_7D,
        p.MA_30D,
        CASE 
            WHEN p.MA_7D > p.MA_30D THEN 'Uptrend'
            WHEN p.MA_7D < p.MA_30D THEN 'Downtrend'
            ELSE 'Neutral'
        END AS TREND_STATUS
    FROM price_trends p
    JOIN {{ ref('transformed_coingecko_data_v') }} c
        ON p.CRYPTO_ID = c.ID
)
SELECT *
FROM trend_analysis
ORDER BY TRADE_DATE DESC
LIMIT 20