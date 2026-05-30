-- Staging model for fact_listings
-- Selects and renames columns for downstream use

with source as (
    select * from {{ source('gold', 'fact_listings') }}
),

renamed as (
    select
        fact_key,
        listing_id,
        host_key,
        location_key,
        property_key,
        date_key,
        snapshot_date,
        city,
        neighbourhood,
        review_scores_rating,
        review_scores_cleanliness,
        review_scores_location,
        number_of_reviews,
        availability_365,
        availability_90,
        availability_30,
        calculated_host_listings_count
    from source
    where listing_id is not null
)

select * from renamed