select
    customer_id
    , subscription_id
    , card_number
    , customer_status as customer_status
    , billing_address
    , billing_email
    , employee_count
    , churn_date
from  {{ source('staging_back_office', 'customer') }}