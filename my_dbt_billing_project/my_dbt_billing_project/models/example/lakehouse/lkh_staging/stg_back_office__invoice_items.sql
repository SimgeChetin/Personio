select
    customer_id
    , invoice_created_month::date as invoice_created_month
    , charge_product_id
    , amount as invoice_amount
    , invoice_id
    , invoice_item_id
    , invoice_period_start::date as invoice_period_start_month
    , invoice_period_end::date as invoice_period_end_month
    , coalesce(tax_amount, 0) as tax_amount
from {{ source('staging_back_office', 'invoices') }}