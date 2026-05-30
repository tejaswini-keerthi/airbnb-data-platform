-- Staging model for dim_host

with source as (
    select * from {{ source('gold', 'dim_host') }}
),

renamed as (
    select
        host_key,
        host_id,
        host_name,
        host_since,
        host_location,
        host_response_time,
        is_superhost,
        host_response_rate_pct,
        host_acceptance_rate_pct,
        host_total_listings_count
    from source
    where host_id is not null
)

select * from renamed