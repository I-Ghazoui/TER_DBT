{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('latest_transformed_coingecko_data_v') }}
)
select
    id,
    name,
    symbol,
    current_price,
    price_change_percentage_24h,
    case 
        when price_change_percentage_24h >= 0 then 'Gainer'
        else 'Loser'
    end as performance
from base
order by price_change_percentage_24h desc
