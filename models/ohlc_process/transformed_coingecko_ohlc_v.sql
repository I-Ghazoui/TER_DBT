WITH transformed_ohlc_data AS (
    SELECT
        *
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA
)

SELECT * FROM transformed_ohlc_data
