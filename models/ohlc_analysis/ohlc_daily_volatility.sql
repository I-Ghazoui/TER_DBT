WITH filtered_cryptos AS (
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        NULLIF(MARKET_CAP, 0) AS MARKET_CAP,
        CIRCULATING_SUPPLY,
        NULLIF(TOTAL_SUPPLY, 0) AS TOTAL_SUPPLY,
        NULLIF(ATH, 0) AS ATH,
        NULLIF(ATL, 0) AS ATL,
        CURRENT_PRICE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
),

daily_data AS (
    SELECT 
        ohlc.CRYPTO_ID AS CRYPTO_ID,
        filtcryp.SYMBOL AS SYMBOL,
        filtcryp.NAME AS NAME,
        filtcryp.MARKET_CAP AS MARKET_CAP,
        filtcryp.CIRCULATING_SUPPLY AS CIRCULATING_SUPPLY,
        filtcryp.TOTAL_SUPPLY AS TOTAL_SUPPLY,
        filtcryp.ATH AS ATH,
        filtcryp.ATL AS ATL,
        filtcryp.CURRENT_PRICE AS CURRENT_PRICE,
        ohlc.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        ohlc.HIGH_PRICE AS HIGH_PRICE,
        ohlc.LOW_PRICE AS LOW_PRICE,
        NULLIF(ohlc.CLOSE_PRICE, 0) AS CLOSE_PRICE
        
    FROM {{ ref('transformed_coingecko_ohlc_v') }} ohlc
    JOIN filtered_cryptos filtcryp
        ON ohlc.CRYPTO_ID = filtcryp.CRYPTO_ID
    
),

ohlc_volatility AS (
    SELECT 
        CRYPTO_ID,
        SYMBOL,
        NAME,
        TRADE_DATE,
        MAX(HIGH_PRICE) AS HIGH_PRICE,
        MIN(LOW_PRICE) AS LOW_PRICE,
        MAX(CLOSE_PRICE) AS CLOSE_PRICE,
        AVG(MARKET_CAP) AS AVG_MARKET_CAP,
        (AVG(CIRCULATING_SUPPLY) / AVG(TOTAL_SUPPLY)) * 100 AS SUPPLY_RATIO_PERCENTAGE,
        (AVG(CURRENT_PRICE) - AVG(ATH)) / AVG(ATH) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
        (AVG(CURRENT_PRICE) - AVG(ATL)) / AVG(ATL) * 100 AS DISTANCE_TO_ATL_PERCENTAGE,
        (AVG(HIGH_PRICE) - AVG(LOW_PRICE)) / AVG(CLOSE_PRICE) * 100 AS DAILY_VOLATILITY_PERCENTAGE
    FROM daily_data
    GROUP BY CRYPTO_ID, SYMBOL, NAME, TRADE_DATE
)

SELECT 
    *
FROM ohlc_volatility
WHERE DAILY_VOLATILITY_PERCENTAGE IS NOT NULL
ORDER BY DAILY_VOLATILITY_PERCENTAGE DESC