-- Staging model for dim_date

with source as (
    select * from {{ source('gold', 'dim_date') }}
),

renamed as (
    select
        date_key,
        date,
        year,
        month,
        quarter,
        day_of_week,
        is_weekend,
        month_name,
        quarter_name
    from source
)

select * from renamed