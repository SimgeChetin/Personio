select
distinct
  cust.customer_id
  , invoiced_subscription_plan.product_name as subscription_plan_name
  , cust.subs_start_billing_year_month
  , cust.subs_end_billing_year_month
  , cust.months_left_to_new_subscription
  , cust.is_subscription_to_renew
  , invoiced_products.product_name as addon_name
  , max(case when invoiced_products.product_name = 'Recruiting' then 1 else 0 end) over(partition by cust.customer_id) as has_recruiting_addon
  -- Example of cross-sell logic
  , case when lower(subscription_plan_name) like '%core plan%' and not has_recruiting_addon = 0 then true else false end as cross_sell_opportunity
from {{ ref('dim_customers_subscriptions_monthly') }} as cust
left join 
{{ ref('int__invoices_products') }} as invoiced_subscription_plan
on cust.customer_id = invoiced_subscription_plan.customer_id
and invoiced_subscription_plan.product_type  = 'Subscription'
left join 
{{ ref('int__invoices_products') }} as invoiced_products
on cust.customer_id = invoiced_products.customer_id
and invoiced_products.product_type = 'application'
where cust.billing_year_month <= add_months(date_trunc('month', current_date()), 4)
and is_subscription_to_renew