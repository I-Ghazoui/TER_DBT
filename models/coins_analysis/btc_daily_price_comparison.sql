
SELECT
    symbol,
    name,
    avg(CURRENT_PRICE) as avg_daily_price,
    date_trunc('day', CREATION_DATE) as price_date

FROM {{ ref('transformed_coingecko_data_v') }}
WHERE symbol = 'btc'
        and name = 'Bitcoin'
GROUP BY symbol, name, price_date
ORDER BY price_date DESC
