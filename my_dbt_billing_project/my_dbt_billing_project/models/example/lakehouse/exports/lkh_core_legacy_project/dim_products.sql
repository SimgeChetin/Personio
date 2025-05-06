select
    product_id
    , product_name
    , product_type
    , price
    , price_per_unit
from {{ ref('stg_back_office__products') }}