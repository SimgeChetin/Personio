select 
    invoice.customer_id
    , invoice.invoice_created_month
    , invoice.invoice_id
    , invoice.invoice_item_id
    , invoice.charge_product_id
    , product.product_name
    , invoice.invoice_amount
    , invoice.invoice_period_start_month
    , invoice.invoice_period_end_month
    , coalesce(case when lower(product.product_type) like '%discount%' then invoice.invoice_amount end, 0) as discount_amount
    , coalesce(case when lower(product.product_type) not like '%discount%' then invoice.invoice_amount end, 0) as invoice_charge_amount
from {{ ref('stg_back_office__invoice_items') }} as invoice
inner join 
{{ ref('stg_back_office__products') }} as product
on invoice.charge_product_id = product.product_id