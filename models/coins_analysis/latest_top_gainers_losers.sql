{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('latest_transformed_coingecko_data_v') }}
    where id is not null
        AND id != ' '
        AND name is not null
        AND name != ' '
        AND symbol is not null
        AND symbol != ' '
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
