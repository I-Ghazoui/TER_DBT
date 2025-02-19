
with daily_prices as (
    select 
        symbol,
        name,
        avg(CURRENT_PRICE) as avg_daily_price,
        date_trunc('day', CREATION_DATE) as price_date
    from {{ ref('transformed_coingecko_data_v') }}
    where symbol = 'btc'
        and name = 'Bitcoin'
    group by 
        symbol,
        name,
        price_date
    order by price_date desc
    limit 2
)

select
    symbol,
    name,
    avg_daily_price as current_daily_price,
    lead(avg_daily_price) over (order by price_date desc) as previous_daily_price
from daily_prices
limit 1