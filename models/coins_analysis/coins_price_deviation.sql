WITH coin_price_deviation AS (
    SELECT
        symbol,
        NAME,
        current_price,
        ath,
        atl,
        (current_price - ath) / ath * 100 AS percent_from_ath,
        (current_price - atl) / atl * 100 AS percent_from_atl,
        creation_date
    FROM {{ ref('transformed_coingecko_data') }}
)

SELECT *
FROM coin_price_deviation
ORDER BY percent_from_ath DESC