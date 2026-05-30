-- Data quality metrics summary
-- Tracks null rates and row counts per snapshot

with fact as (
    select * from {{ ref('stg_fact_listings') }}
),

metrics as (
    select
        snapshot_date,
        city,
        count(*)                                                    as total_rows,
        count(review_scores_rating)                                 as rows_with_rating,
        count(neighbourhood)                                        as rows_with_neighbourhood,
        round(
            count(review_scores_rating) * 100.0 / nullif(count(*), 0), 2
        )                                                           as rating_completeness_pct,
        round(avg(availability_365), 1)                             as avg_availability,
        round(avg(number_of_reviews), 1)                            as avg_reviews,
        current_timestamp()                                         as measured_at
    from fact
    group by snapshot_date, city
)

select * from metrics