{{ config(
        materialized='incremental'
        , unique_key = ['event_id']
    )
}}

select
    event_id
    , event_at
    , event_category
    , event_type
    , event_details
    , event_details:customer_id::text as customer_id
    , coalesce(event_details:subscription_id::text, event_details:subscription:id::text) as subscription_id
    , event_details:subscription:amount as subscription_amount
    , event_details:subscription:billing_start_month::date as subscription_billing_start_month
    , event_details:subscription:discount as subscription_discount
    , event_details:subscription:plan_name::text as subscription_plan_name
    , event_details:subscription:product_id::text as subscription_product_id
    , event_details:subscription:billing_end_month::date as subscription_billing_end_month
    , event_details:product:billing_start_month::date as product_billing_start_month
    , event_details:product:billing_end_month::date as product_billing_end_month
    , event_details:product:id::text as product_id
    , event_details:product:name::text as product_name
    , event_details:product:amount as product_amount
    , event_details:payment_method:new_value::text as payment_method_changed_to
    , event_details:payment_method:old_value::text as payment_method_changed_from
    , current_timestamp(0) as _dbt_insert_at
from {{ source('staging_back_office', 'events') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where event_at > coalesce((select max({{ this }}.event_at) from {{ this }}), '1900-01-01 00:00:00')
{% endif %}