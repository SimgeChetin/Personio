{{
    config(
        materialized='incremental'
        , unique_key = ['invoice_id']
    )
}}

with incremental_events_selection as (
    select 
    * 
    from
{{ ref('stg_back_office__events') }}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where _dbt_insert_at > (select max({{ this }}._dbt_insert_at) from {{ this }})
{% endif %}
)

select
    invoice.customer_id
    , invoice.invoice_created_month
    , invoice.invoice_id
    , sum(invoice.invoice_charge_amount) as total_invoice_charge_amount
    , sum(invoice.discount_amount) as total_discount_amount
    , total_invoice_charge_amount - total_discount_amount as total_invoice_charge_amount_with_discount
    , sum(case when application_upgrade_events.event_type = 'application-added' then application_upgrade_events.product_amount end) as upgrade_invoice_amount
    , sum(case when application_downgrade_events.event_type = 'application-removed' then application_downgrade_events.product_amount end) as downgrade_invoice_amount
    , current_timestamp(0) as _dbt_insert_at
from {{ ref('int__invoices_products') }} as invoice
left join incremental_events_selection as application_upgrade_events
    on invoice.charge_product_id = application_upgrade_events.product_id
        and invoice.customer_id = application_upgrade_events.customer_id
        and invoice.invoice_created_month = application_upgrade_events.product_billing_start_month
        and application_upgrade_events.event_type in ('application-added')
left join incremental_events_selection as application_downgrade_events
    on invoice.customer_id = application_downgrade_events.customer_id
        and invoice.invoice_created_month = application_downgrade_events.product_billing_end_month
        and application_downgrade_events.event_type in ('application-removed')
group by all