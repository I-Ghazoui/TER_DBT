WITH filtered_cryptos AS (
    SELECT 
        ID,
        SYMBOL
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
),

daily_averages AS (
    SELECT 
        ohlc.CRYPTO_ID,
        ohlc.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(ohlc.OPEN_PRICE) AS AVG_OPEN_PRICE,
        AVG(ohlc.HIGH_PRICE) AS AVG_HIGH_PRICE,
        AVG(ohlc.LOW_PRICE) AS AVG_LOW_PRICE,
        AVG(ohlc.CLOSE_PRICE) AS AVG_CLOSE_PRICE
    FROM {{ ref('transformed_coingecko_ohlc_v') }} ohlc
    JOIN filtered_cryptos fc
        ON ohlc.CRYPTO_ID = fc.ID
    GROUP BY ohlc.CRYPTO_ID, ohlc.OHLC_TIMESTAMP::DATE
),

price_trends AS (
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        AVG_CLOSE_PRICE,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_averages
),

trend_analysis AS (
    SELECT 
        p.CRYPTO_ID,
        c.SYMBOL,
        p.TRADE_DATE,
        p.AVG_CLOSE_PRICE,
        p.MA_7D,
        p.MA_30D,
        CASE 
            WHEN p.MA_7D > p.MA_30D THEN 'Uptrend'
            WHEN p.MA_7D < p.MA_30D THEN 'Downtrend'
            ELSE 'Neutral'
        END AS TREND_STATUS
    FROM price_trends p
    JOIN filtered_cryptos c
        ON p.CRYPTO_ID = c.ID
),

final_results AS (
    SELECT 
        SYMBOL,
        TRADE_DATE,
        AVG(AVG_CLOSE_PRICE) AS FINAL_AVG_CLOSE_PRICE,
        AVG(MA_7D) AS FINAL_MA_7D,
        AVG(MA_30D) AS FINAL_MA_30D,
        CASE 
            WHEN AVG(MA_7D) > AVG(MA_30D) THEN 'Uptrend'
            WHEN AVG(MA_7D) < AVG(MA_30D) THEN 'Downtrend'
            ELSE 'Neutral'
        END AS FINAL_TREND_STATUS
    FROM trend_analysis
    GROUP BY SYMBOL, TRADE_DATE
)

SELECT 
    SYMBOL,
    TRADE_DATE,
    FINAL_AVG_CLOSE_PRICE AS AVG_CLOSE_PRICE,
    FINAL_MA_7D AS MA_7D,
    FINAL_MA_30D AS MA_30D,
    FINAL_TREND_STATUS AS TREND_STATUS
FROM final_results
ORDER BY SYMBOL ASC, TRADE_DATE DESC