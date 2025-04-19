select
    customer_details.customer_id
    , customer_details.customer_status
    , customer_details.billing_address
    , customer_details.billing_email
    , customer_details.employee_count
    , date_trunc('month', customer_details.churn_date) as customer_churn_billing_year_month
    , subscriptions.subscription_id
    , subscriptions.subs_start_date as subs_start_billing_year_month
    , subscriptions.subs_end_date as subs_end_billing_year_month
    , subs_end_billing_year_month = '{{ var("in_long_time") }}' as is_subscription_to_renew
    , subscriptions.subs_billing_frequency
    , subscriptions.subs_amount
    , subscriptions.subs_discount
    , subscriptions.subs_plan_name
    , subscriptions.subscription_status
from {{ ref('stg_back_office__customers') }} as customer_details
inner join
{{ ref('stg_back_office__subscriptions') }} as subscriptions
    on customer_details.customer_id = subscriptions.customer_id