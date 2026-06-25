{{ config(
    materialized = 'view',
    alias = 'logs',
    schema = 'staging'
)
}}

with raw_logs as (
    select *
    from {{ source('ethereum', 'logs') }}
)
select id as log_id,
    transaction_hash,
    block_number,
    log_index,
    address as contract_address,
    data,
    topics
from raw_logs