select
    payment_id
    , invoice_id
    , subscription_id
    , payment_amount
    , payment_date
    , payment_method
    , date_trunc('month', payment_date) as payment_year_month
from {{ ref('stg_back_office__payments') }}