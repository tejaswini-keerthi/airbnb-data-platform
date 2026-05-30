-- Neighbourhood-level summary mart
-- Aggregates listing metrics by neighbourhood and snapshot

with enriched as (
    select * from {{ ref('int_listing_enriched') }}
),

summary as (
    select
        neighbourhood,
        city,
        country,
        snapshot_date,

        count(distinct listing_id)                          as total_listings,
        round(avg(review_scores_rating), 2)                 as avg_rating,
        round(avg(review_scores_cleanliness), 2)            as avg_cleanliness_score,
        round(avg(availability_365), 0)                     as avg_availability_days,
        sum(number_of_reviews)                              as total_reviews,

        -- superhost metrics
        count(case when is_superhost = true then 1 end)     as superhost_listings,
        round(
            count(case when is_superhost = true then 1 end) * 100.0
            / nullif(count(distinct listing_id), 0), 1
        )                                                   as superhost_pct,

        -- room type breakdown
        count(case when room_type = 'Entire home/apt' then 1 end)   as entire_home_listings,
        count(case when room_type = 'Private room' then 1 end)      as private_room_listings,
        count(case when room_type = 'Shared room' then 1 end)       as shared_room_listings

    from enriched
    group by neighbourhood, city, country, snapshot_date
)

select * from summary