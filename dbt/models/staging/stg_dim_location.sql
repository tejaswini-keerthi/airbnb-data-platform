-- Staging model for dim_location

with source as (
    select * from {{ source('gold', 'dim_location') }}
),

renamed as (
    select
        location_key,
        neighbourhood,
        city,
        country,
        latitude,
        longitude
    from source
    where neighbourhood is not null
)

select * from renamed