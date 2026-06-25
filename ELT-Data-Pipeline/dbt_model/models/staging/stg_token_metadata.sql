{{ config(
    materialized = 'view',
    alias = 'token_metadata',
    schema = 'staging'
) }} with raw_token_metadata as (
    select *
    from {{ source('ethereum', 'token_metadata') }}
)
select token_address,
    token_name,
    symbol,
    decimals,
    total_supply,
    supply_source
from raw_token_metadata