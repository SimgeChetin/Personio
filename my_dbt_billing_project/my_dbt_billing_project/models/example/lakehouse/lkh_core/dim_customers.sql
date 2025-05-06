select
    customer_id
    , subscription_id
    , card_number
    , customer_status
    , billing_address
    , billing_email
    , employee_count
    , churn_date
from {{ ref('stg_back_office__customers') }}