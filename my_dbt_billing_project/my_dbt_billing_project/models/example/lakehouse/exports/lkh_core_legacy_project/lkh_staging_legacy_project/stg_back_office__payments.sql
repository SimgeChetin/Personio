select
    payment_id
    , invoice_id
    , subscription_id
    , payment_amount
    , payment_date::date as payment_date
    , payment_method
from {{ source('staging_back_office', 'payments') }}