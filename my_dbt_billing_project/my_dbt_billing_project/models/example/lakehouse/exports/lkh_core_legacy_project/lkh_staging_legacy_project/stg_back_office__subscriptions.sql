select
    customer_id
    , subscription_id
    , subs_start_date::date as subs_start_date
    , coalesce(subs_end_date::date, '{{ var("in_long_time") }}'::date) as subs_end_date
    , subs_billing_frequency
    , subs_amount
    , subs_discount
    , subs_plan_name
    , status as subscription_status
from {{ source('staging_back_office', 'subscriptions') }}