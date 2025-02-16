{{ config( materialized='view' ) }}

WITH COINGECKO_COINS_DATA_TRANSFORMED AS (
    SELECT
        ID,
        SYMBOL,
        NAME,
        IMAGE,
        CURRENT_PRICE,
        MARKET_CAP,
        MARKET_CAP_RANK,
        FULLY_DILUTED_VALUATION,
        TOTAL_VOLUME,
        HIGH_24H,
        LOW_24H,
        PRICE_CHANGE_24H,
        PRICE_CHANGE_PERCENTAGE_24H,
        MARKET_CAP_CHANGE_24H,
        MARKET_CAP_CHANGE_PERCENTAGE_24H,
        CIRCULATING_SUPPLY,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        ATH,
        ATH_CHANGE_PERCENTAGE,
        
        -- Convert date fields from VARCHAR to TIMESTAMP_NTZ
        TRY_TO_TIMESTAMP_NTZ(ATH_DATE) AS ATH_DATE,
        TRY_TO_TIMESTAMP_NTZ(ATL_DATE) AS ATL_DATE,
        TRY_TO_TIMESTAMP_NTZ(LAST_UPDATED) AS LAST_UPDATED,
        
        ATL,
        ATL_CHANGE_PERCENTAGE,
        
        -- Convert ROI: If NULL or improperly formatted, replace with NULL
        NULLIF(ROI, '') AS ROI,

        -- Keep creation date as-is
        CREATION_DATE

    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_COINS_DATA

    HAVING CREATION_DATE = MAX(CREATION_DATE)
)

SELECT * FROM COINGECKO_COINS_DATA_TRANSFORMED
