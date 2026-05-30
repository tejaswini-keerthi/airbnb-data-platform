-- Enriched listing combining fact with property and host dimensions

with fact as (
    select * from {{ ref('stg_fact_listings') }}
),

property as (
    select * from {{ ref('stg_dim_property') }}
),

host as (
    select * from {{ ref('stg_dim_host') }}
),

location as (
    select * from {{ ref('stg_dim_location') }}
),

enriched as (
    select
        f.fact_key,
        f.listing_id,
        f.snapshot_date,
        f.city,
        f.neighbourhood,
        f.review_scores_rating,
        f.review_scores_cleanliness,
        f.review_scores_location,
        f.number_of_reviews,
        f.availability_365,
        f.availability_30,
        f.calculated_host_listings_count,

        -- property attributes
        p.property_type,
        p.room_type,
        p.accommodates,
        p.bedrooms,
        p.bathrooms,
        p.beds,
        p.price_tier,
        p.minimum_nights,
        p.instant_bookable,

        -- host attributes
        h.host_id,
        h.host_name,
        h.is_superhost,
        h.host_response_rate_pct,
        h.host_acceptance_rate_pct,
        h.host_since,

        -- location attributes
        l.country,
        l.latitude,
        l.longitude

    from fact f
    left join property p on f.property_key = p.property_key
    left join host h on f.host_key = h.host_key
    left join location l on f.location_key = l.location_key
)

select * from enriched