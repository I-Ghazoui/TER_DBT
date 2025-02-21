{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('latest_transformed_coingecko_data_v') }}
)
select
    id,
    name,
    symbol,
    market_cap,
    fully_diluted_valuation,
    case 
        when fully_diluted_valuation is not null and fully_diluted_valuation > 0 then
            (fully_diluted_valuation - market_cap) / fully_diluted_valuation * 100
        else null
    end as fdv_gap_percentage
from base
order by fdv_gap_percentage desc
