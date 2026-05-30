-- Host performance mart
-- Aggregates metrics per host across all their listings

with enriched as (
    select * from {{ ref('int_listing_enriched') }}
),

host_metrics as (
    select
        host_id,
        host_name,
        is_superhost,
        host_since,
        host_response_rate_pct,
        host_acceptance_rate_pct,
        city,
        snapshot_date,

        count(distinct listing_id)              as total_listings,
        round(avg(review_scores_rating), 2)     as avg_rating,
        sum(number_of_reviews)                  as total_reviews,
        round(avg(availability_365), 0)         as avg_availability_days,
        max(accommodates)                       as max_accommodates

    from enriched
    where host_id is not null
    group by
        host_id, host_name, is_superhost, host_since,
        host_response_rate_pct, host_acceptance_rate_pct,
        city, snapshot_date
)

select * from host_metrics