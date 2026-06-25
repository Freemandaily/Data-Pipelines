{{ config(
    materialized = 'view',
    alias = 'blocks',
    schema = 'staging'
) }} with raw_blocks as (
    select *
    from {{ source('ethereum', 'blocks') }}
)
select number as block_number,
    timestamp as block_timestamp,
    hash as block_hash,
    gas_used,
    gas_limit
from raw_blocks