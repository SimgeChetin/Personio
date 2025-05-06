select
    product_id
    , product_name
    , product_type
    , price
    , price_per_unit
from {{ source('staging_back_office', 'products') }}