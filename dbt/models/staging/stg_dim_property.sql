-- Staging model for dim_property

with source as (
    select * from {{ source('gold', 'dim_property') }}
),

renamed as (
    select
        property_key,
        listing_id,
        listing_name,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        bathrooms,
        beds,
        minimum_nights,
        maximum_nights,
        instant_bookable,
        snapshot_date,
        price_tier
    from source
    where listing_id is not null
)

select * from renamed