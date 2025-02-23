CREATE OR REPLACE VIEW TER_DATABASE.TER_ANALYSIS_DATA.OHCL_DAILY_VOLATILITY (
    CRYPTO_ID,
    SYMBOL,
    NAME,
    AVG_DAILY_VOLATILITY,
    AVG_MARKET_CAP,
    SUPPLY_RATIO_PERCENTAGE,
    DISTANCE_TO_ATH_PERCENTAGE,
    DISTANCE_TO_ATL_PERCENTAGE
) AS
WITH filtered_cryptos AS (
    -- Filtrer uniquement les cryptomonnaies spécifiées
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
),
daily_data AS (
    -- Calculer les prix maximum, minimum et de clôture quotidiens
    SELECT 
        c.CRYPTO_ID,
        c.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        MAX(c.HIGH_PRICE) AS HIGH_PRICE, -- Prix maximum quotidien
        MIN(c.LOW_PRICE) AS LOW_PRICE,  -- Prix minimum quotidien
        MAX(c.CLOSE_PRICE) AS CLOSE_PRICE -- Prix de clôture quotidien
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA c
    JOIN filtered_cryptos f
        ON c.CRYPTO_ID = f.CRYPTO_ID
    GROUP BY c.CRYPTO_ID, c.OHLC_TIMESTAMP::DATE
),
ohlc_volatility AS (
    -- Calculer la volatilité quotidienne en pourcentage
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        HIGH_PRICE,
        LOW_PRICE,
        CLOSE_PRICE,
        (HIGH_PRICE - LOW_PRICE) / NULLIF(CLOSE_PRICE, 0) * 100 AS DAILY_VOLATILITY_PERCENTAGE
    FROM daily_data
),
coin_metadata AS (
    -- Extraire les métadonnées des cryptomonnaies
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME,
        MARKET_CAP,
        CIRCULATING_SUPPLY,
        TOTAL_SUPPLY,
        ATH,
        ATL,
        CURRENT_PRICE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
)
-- Résultat final : calculer les métriques agrégées pour les cryptomonnaies spécifiées
SELECT 
    v.CRYPTO_ID,
    m.SYMBOL,
    m.NAME,
    AVG(v.DAILY_VOLATILITY_PERCENTAGE) AS AVG_DAILY_VOLATILITY,
    AVG(NULLIF(m.MARKET_CAP, 0)) AS AVG_MARKET_CAP,
    (m.CIRCULATING_SUPPLY / NULLIF(m.TOTAL_SUPPLY, 0)) * 100 AS SUPPLY_RATIO_PERCENTAGE,
    (m.CURRENT_PRICE - m.ATH) / NULLIF(m.ATH, 0) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
    (m.CURRENT_PRICE - m.ATL) / NULLIF(m.ATL, 0) * 100 AS DISTANCE_TO_ATL_PERCENTAGE
FROM ohlc_volatility v
JOIN coin_metadata m
    ON v.CRYPTO_ID = m.CRYPTO_ID
GROUP BY v.CRYPTO_ID, m.SYMBOL, m.NAME, m.CIRCULATING_SUPPLY, m.TOTAL_SUPPLY, m.ATH, m.ATL, m.CURRENT_PRICE
HAVING AVG(v.DAILY_VOLATILITY_PERCENTAGE) IS NOT NULL -- Exclure les cryptomonnaies sans volatilité
ORDER BY AVG_DAILY_VOLATILITY DESC