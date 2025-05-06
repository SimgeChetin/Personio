select
    customer_subscription.customer_id
    , customer_subscription.billing_address
    , customer_subscription.billing_email
    , customer_subscription.employee_count
    , customer_subscription.customer_churn_billing_year_month
    , customer_subscription.subscription_id
    , customer_subscription.subs_start_billing_year_month
    , customer_subscription.subs_end_billing_year_month
    , customer_subscription.subs_billing_frequency
    , customer_subscription.subs_amount
    , customer_subscription.subs_discount
    , customer_subscription.subs_plan_name
    , customer_subscription.is_subscription_to_renew
    , date_trunc('month', date_day) as billing_year_month
    , case when billing_year_month<> subs_end_billing_year_month then 'active'
        else customer_subscription.subscription_status
    end as subscription_status
    , case when billing_year_month<> customer_churn_billing_year_month then 'active'
        else customer_subscription.customer_status
    end as customer_status
    , case when customer_subscription.subs_start_billing_year_month > date_trunc('month', current_date()) 
        then 
        abs(datediff('month', date_trunc('month', current_date()), customer_subscription.subs_start_billing_year_month)) 
    end as 
    months_left_to_new_subscription
    , {{ dbt_utils.generate_surrogate_key([
	    "customer_subscription.customer_id",
	    "customer_subscription.subscription_id",
        "billing_year_month"
	    ]) }} as customer_subscription_year_month_pk
from
{{ ref('int__customers_subscriptions') }} as customer_subscription
inner join {{ ref('dim_dates') }}
on dates.date_day between subs_start_billing_year_month  and subs_end_billing_year_month
---Show up to 6 months ahead of current billing month
where billing_year_month <= add_months(date_trunc('month', current_date()), 6) 
group by all