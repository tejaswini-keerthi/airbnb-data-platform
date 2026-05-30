{% snapshot dim_property_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='listing_id',
        strategy='check',
        check_cols=['price_tier', 'property_type', 'room_type', 'accommodates', 'bedrooms'],
    )
}}

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

from {{ ref('stg_dim_property') }}

{% endsnapshot %}