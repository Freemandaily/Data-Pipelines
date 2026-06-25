{{ config(
    materialized = 'view',
    alias = 'traces',
    schema = 'staging'
) }} with raw_traces as (
    select *
    from {{ source('ethereum', 'traces') }}
)
select id as trace_id,
    transaction_hash,
    block_number,
    trace_address,
    call_type,
    error as trace_error,
    from_address,
    to_address,
    value as eth_value
from raw_traces